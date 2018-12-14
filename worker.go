package gocelery

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

// defaultWaitTime to fallback to if the provided waitTime is 0.
const defaultWaitTime = 1

// CeleryWorker represents distributed task worker.
// Not thread safe. Shouldn't be used from within multiple go routines.
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	numWorkers      int
	waitTimeMS      int
	registeredTasks map[string]interface{}
	taskLock        sync.RWMutex
	stopChannel     chan struct{}
	workWG          sync.WaitGroup
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int, waitTimeMS int) *CeleryWorker {
	if waitTimeMS < 1 {
		waitTimeMS = defaultWaitTime
	}

	return &CeleryWorker{
		broker:          broker,
		backend:         backend,
		numWorkers:      numWorkers,
		registeredTasks: make(map[string]interface{}),
		waitTimeMS:      waitTimeMS,
	}
}

// StartWorker starts celery worker
func (w *CeleryWorker) StartWorker() {

	w.stopChannel = make(chan struct{})
	w.workWG.Add(w.numWorkers)
	ticker := time.NewTicker(time.Millisecond * time.Duration(w.waitTimeMS))

	for i := 0; i < w.numWorkers; i++ {
		go func(workerID int) {
			defer w.workWG.Done()
			for {
				select {
				case <-w.stopChannel:
					ticker.Stop()
					return
				case <-ticker.C:
					// process messages
					taskMessage, err := w.broker.GetTaskMessage()
					if err != nil || taskMessage == nil {
						continue
					}

					// run task
					resultMsg, err := w.RunTask(taskMessage)
					if err == nil {
						// happy path
						w.storeResult(taskMessage.ID, resultMsg)

						// release the result resources
						releaseResultMessage(resultMsg)
						continue
					}

					taskMessage.Tries++
					if err != ErrTaskRetryable || !taskMessage.isRetryable() {
						// not a retryable error. move on
						res := getResultMessage(nil)
						res.Error = err.Error()
						w.storeResult(taskMessage.ID, res)
						releaseResultMessage(res)
						continue
					}

					// retryable error enqueue the task again
					enc, err := taskMessage.Encode()
					if err != nil {
						log.Println(fmt.Errorf("failed to encode Task Message: %v", err))
						continue
					}

					err = w.broker.SendCeleryMessage(getCeleryMessage(enc))
					if err != nil {
						log.Println(fmt.Errorf("failed to enqueue Task: %v", err))
					}
				}
			}
		}(i)
	}
}

func (w *CeleryWorker) storeResult(taskID string, result *ResultMessage) {
	// push result to backend
	err := w.backend.SetResult(taskID, result)
	if err != nil {
		log.Println(err)
	}
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() {
	for i := 0; i < w.numWorkers; i++ {
		w.stopChannel <- struct{}{}
	}
	w.workWG.Wait()
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.numWorkers
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, task interface{}) {
	w.taskLock.Lock()
	defer w.taskLock.Unlock()
	w.registeredTasks[name] = task
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) interface{} {
	w.taskLock.RLock()
	defer w.taskLock.RUnlock()
	task, ok := w.registeredTasks[name]
	if !ok {
		return nil
	}
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*ResultMessage, error) {

	// get task
	task := w.GetTask(message.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}

	// convert to task interface
	taskInterface, ok := task.(CeleryTask)
	if ok {
		// copy the task to avoid race conditions caused by task state
		if taskInterfaceCpy, err := taskInterface.Copy(); err != nil {
			return nil, err
		} else {
			taskInterface = taskInterfaceCpy
		}
		//log.Println("using task interface")
		if err := taskInterface.ParseKwargs(message.Kwargs); err != nil {
			return nil, err
		}
		val, err := taskInterface.RunTask()
		if err != nil {
			return nil, err
		}

		return getResultMessage(val), err
	}
	//log.Println("using reflection")

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(task)
	return runTaskFunc(&taskFunc, message)
}

func runTaskFunc(taskFunc *reflect.Value, message *TaskMessage) (*ResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}
	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}
	//defer releaseResultMessage(ResultMessage)
	return getReflectionResultMessage(&res[0]), nil
}
