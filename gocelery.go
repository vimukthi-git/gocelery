package gocelery

import (
	"fmt"
	"time"
)

// errString implements error interface.
type errString string

// Error returns the error in string.
func (e errString) Error() string {
	return string(e)
}

// ErrTaskRetryable indicates that the task failed but need to be retried again.
const ErrTaskRetryable = errString("task failed but retryable")

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(*CeleryMessage) error
	GetTaskMessage() (*TaskMessage, error) // must be non-blocking
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int, workerWaitTimeMS int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers, workerWaitTimeMS),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker() {
	cc.worker.StartWorker()
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() {
	cc.worker.StopWorker()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task Task) (*AsyncResult, error) {
	if len(task.Args) > 0 && len(task.Kwargs) > 0 {
		return nil, fmt.Errorf("either args or kwargs need to be set, but not both")
	}

	celeryTask := getTaskMessage(task)
	return cc.delay(celeryTask)
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		taskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// Copy - is used to safely create and execute a copy of a task (stateful)
	// in a worker when there are multiple workers working on the same type of task but with different internal state.
	Copy() (CeleryTask, error)

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method to run
	RunTask() (interface{}, error)
}

// AsyncResult is pending result
type AsyncResult struct {
	taskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from redis
// It blocks for period of time set by timeout and return error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.taskID)
			return nil, err
		case <-ticker.C:
			if ready := ar.Ready(); !ready {
				continue
			}

			if ar.result.Error != "" {
				return nil, fmt.Errorf(ar.result.Error)
			}

			return ar.result.Result, nil
		}
	}
}

// AsyncGet gets actual result from backend
// returns the err if the result is not available yet.
// Always check Ready if the result is ready to be consumed
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	// process
	val, err := ar.backend.GetResult(ar.taskID)
	if err != nil {
		return nil, err
	}

	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() bool {
	if ar.result != nil {
		return true
	}
	val, err := ar.backend.GetResult(ar.taskID)
	if err != nil {
		return false
	}

	ar.result = val
	return true
}
