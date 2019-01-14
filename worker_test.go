package gocelery

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// add is test task method
func add(a int, b int) int {
	return a + b
}

// redisWorker creates redis celery worker
func redisWorker(numWorkers int) *CeleryWorker {
	broker := NewRedisCeleryBroker("redis://localhost:6379")
	backend := NewRedisCeleryBackend("redis://localhost:6379")
	celeryWorker := NewCeleryWorker(broker, backend, numWorkers, 1)
	return celeryWorker
}

// inMemoryWorker creates inmemory celery worker
func inMemoryWorker(numWorkers int) *CeleryWorker {
	broker := NewInMemoryBroker()
	backend := NewInMemoryBackend()
	celeryWorker := NewCeleryWorker(broker, backend, numWorkers, 1)
	return celeryWorker
}

func levelDBWorker(t *testing.T, numWorkers int) (*CeleryWorker, func()) {
	levelDB, funcC := getLevelDB(t)
	broker := NewLevelDBBroker(levelDB, "test")
	backend := NewLevelDBBackend(levelDB)
	return NewCeleryWorker(broker, backend, numWorkers, 1), funcC
}

func getWorkers(t *testing.T, numWorkers int) ([]*CeleryWorker, func()) {
	levelDBWorker, funcC := levelDBWorker(t, numWorkers)
	return []*CeleryWorker{
		levelDBWorker,
		inMemoryWorker(numWorkers),
		redisWorker(numWorkers),
	}, funcC
}

// registerTask registers add test task
func registerTask(celeryWorker *CeleryWorker) string {
	taskName := "add"
	registeredTask := add
	celeryWorker.Register(taskName, registeredTask)
	return taskName
}

func runTestForEachWorker(testFunc func(celeryWorker *CeleryWorker, numWorkers int) error, numWorkers int, t *testing.T) {
	workers, funcC := getWorkers(t, numWorkers)
	defer funcC()
	for _, worker := range workers {
		err := testFunc(worker, numWorkers)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestRegisterTask(t *testing.T) {
	runTestForEachWorker(registerTaskTest, 1, t)
}

func registerTaskTest(celeryWorker *CeleryWorker, numWorkers int) error {
	taskName := registerTask(celeryWorker)
	receivedTask := celeryWorker.GetTask(taskName)
	if receivedTask == nil {
		return errors.New("failed to retrieve task")
	}
	return nil
}

func TestRunTask(t *testing.T) {
	runTestForEachWorker(runTaskTest, 1, t)
}

func runTaskTest(celeryWorker *CeleryWorker, numWorkers int) error {
	taskName := registerTask(celeryWorker)
	// prepare args
	args := []interface{}{
		rand.Int(),
		rand.Int(),
	}
	// Run task normally
	res := add(args[0].(int), args[1].(int))
	// construct task message
	taskMessage := &TaskMessage{
		ID:     generateUUID(),
		Task:   taskName,
		Args:   args,
		Kwargs: nil,
		Tries:  1,
	}
	resultMsg, err := celeryWorker.RunTask(taskMessage)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to run celery task %v: %v", taskMessage, err))
	}
	reflectRes := resultMsg.Result.(int64)
	// check result
	if int64(res) != reflectRes {
		return errors.New(fmt.Sprintf("reflect result %v is different from normal result %v", reflectRes, res))
	}
	return nil
}

func TestNumWorkers(t *testing.T) {
	numWorkers := rand.Intn(10)
	runTestForEachWorker(numWorkersTest, numWorkers, t)
}

func numWorkersTest(celeryWorker *CeleryWorker, numWorkers int) error {
	celeryNumWorkers := celeryWorker.GetNumWorkers()
	if numWorkers != celeryNumWorkers {
		return errors.New(fmt.Sprintf("number of workers are different: %d vs %d", numWorkers, celeryNumWorkers))
	}
	return nil
}

func TestStartStop(t *testing.T) {
	numWorkers := rand.Intn(10)
	runTestForEachWorker(startStopTest, numWorkers, t)
}

func startStopTest(celeryWorker *CeleryWorker, numWorkers int) error {
	_ = registerTask(celeryWorker)
	celeryWorker.StartWorker()
	time.Sleep(100 * time.Millisecond)
	celeryWorker.StopWorker()
	return nil
}

type retryableTask struct {
	fail     bool
	failFor  int
	attempts int
}

func (r *retryableTask) Copy() (CeleryTask, error) { return r, nil }

func (r *retryableTask) ParseKwargs(map[string]interface{}) error { return nil }

func (r *retryableTask) RunTask() (interface{}, error) {
	r.attempts++
	if !r.fail {
		return true, nil
	}

	if r.attempts <= r.failFor {
		return nil, ErrTaskRetryable
	}

	return true, nil
}

func runRetryableTask(t *testing.T, c *CeleryClient, task Task, expectedAttempts int, shouldFail bool) {
	res, err := c.Delay(task)
	if err != nil {
		t.Fatal(err)
	}

	_, err = res.Get(time.Second * 5 * time.Duration(expectedAttempts+1))
	if err != nil {
		if shouldFail {
			return
		}

		t.Fatalf("unexpected error: %v", err)
	}

	retryTask := c.worker.GetTask(task.Name).(*retryableTask)
	if retryTask.attempts != expectedAttempts {
		t.Fatalf("Attempts: Got=%v; Expected=%v", retryTask.attempts, expectedAttempts)
	}
}

func checkRetryTask(t *testing.T, c *CeleryClient, taskName string) {
	// dont fail
	retryTask := &retryableTask{fail: false}
	c.Register(taskName, retryTask)
	runRetryableTask(t, c, Task{Name: taskName}, 1, false)

	// fail for 2 times with 3 retries
	retryTask = &retryableTask{fail: true, failFor: 2}
	c.Register(taskName, retryTask)
	runRetryableTask(t, c, Task{Name: taskName, Settings: DefaultSettings()}, 3, false)

	// fail for 3 times with 3 retries
	retryTask = &retryableTask{fail: true, failFor: 3}
	c.Register(taskName, retryTask)
	runRetryableTask(t, c, Task{Name: taskName, Settings: DefaultSettings()}, 2, true)
}

func TestCeleryWorker_RunTask_retries(t *testing.T) {
	ws, funcC := getWorkers(t, 10)
	defer funcC()
	for i, w := range ws {
		c, _ := NewCeleryClient(w.broker, w.backend, w.numWorkers, w.waitTimeMS)
		c.worker = w
		go c.StartWorker()
		checkRetryTask(t, c, fmt.Sprintf("task-%v", i))
		c.StopWorker()
	}
}
