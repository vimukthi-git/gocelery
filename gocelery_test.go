package gocelery

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func multiply(a int, b int) int {
	return a * b
}

type multiplyKwargs struct {
	a int
	b int
}

func (m *multiplyKwargs) Copy() (CeleryTask, error) {
	return &multiplyKwargs{m.a, m.b}, nil
}

func (m *multiplyKwargs) ParseKwargs(kwargs map[string]interface{}) error {
	kwargA, ok := kwargs["a"]
	if !ok {
		return fmt.Errorf("undefined kwarg a")
	}
	kwargAFloat, ok := kwargA.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg a")
	}
	m.a = int(kwargAFloat)
	kwargB, ok := kwargs["b"]
	if !ok {
		return fmt.Errorf("undefined kwarg b")
	}
	kwargBFloat, ok := kwargB.(float64)
	if !ok {
		return fmt.Errorf("malformed kwarg b")
	}
	m.b = int(kwargBFloat)
	return nil
}

func (m *multiplyKwargs) RunTask() (interface{}, error) {
	result := m.a * m.b
	return result, nil
}

type stateFulTask struct {
	copyerror bool
	state     map[string]interface{}
}

func (st *stateFulTask) Copy() (CeleryTask, error) {
	if st.copyerror {
		return nil, errors.New("dummy copying error")
	}
	newState := make(map[string]interface{})
	for k, v := range st.state {
		newState[k] = v
	}
	return &stateFulTask{st.copyerror, newState}, nil
}

func (st *stateFulTask) ParseKwargs(state map[string]interface{}) error {
	// modify internal state to test race conditions
	st.state = state
	return nil
}

func (st *stateFulTask) RunTask() (interface{}, error) {
	log.Print(st.state["val"])
	return st.state["val"], nil
}

func getAMQPClient() (*CeleryClient, error) {
	amqpBroker := NewAMQPCeleryBroker("amqp://")
	amqpBackend := NewAMQPCeleryBackend("amqp://")
	return NewCeleryClient(amqpBroker, amqpBackend, 4, 100)
}

func getRedisClient() (*CeleryClient, error) {
	redisBroker := NewRedisCeleryBroker("redis://localhost:6379")
	redisBackend := NewRedisCeleryBackend("redis://localhost:6379")
	return NewCeleryClient(redisBroker, redisBackend, 1, 100)
}

func getInMemoryClient(numWorkers int) (*CeleryClient, error) {
	inMemoryBroker := NewInMemoryBroker()
	inMemoryBackend := NewInMemoryBackend()
	return NewCeleryClient(inMemoryBroker, inMemoryBackend, numWorkers, 500)
}

func getLevelDBClient(numWorkers int, db *leveldb.DB, queue string) (*CeleryClient, error) {
	broker := NewLevelDBBroker(db, queue)
	backend := NewLevelDBBackend(db)
	return NewCeleryClient(broker, backend, numWorkers, 100)
}

func getClients(db *leveldb.DB, queue string) ([]*CeleryClient, error) {
	redisClient, err := getRedisClient()
	if err != nil {
		return nil, err
	}
	amqpClient, err := getAMQPClient()
	if err != nil {
		return nil, err
	}
	inMemoryClient, err := getInMemoryClient(1)
	if err != nil {
		return nil, err
	}

	levelDBClient, err := getLevelDBClient(1, db, queue)
	if err != nil {
		return nil, err
	}

	return []*CeleryClient{
		redisClient,
		amqpClient,
		inMemoryClient,
		levelDBClient,
	}, nil
}

func debugLog(client *CeleryClient, format string, args ...interface{}) {
	pre := fmt.Sprintf("client[%p] - ", client)
	log.Printf(pre+format, args...)
}

func TestWorkerClient(t *testing.T) {
	levelDB, funcC := getLevelDB(t)
	defer funcC()

	// prepare clients
	celeryClients, err := getClients(levelDB, "test")
	if err != nil {
		t.Errorf("failed to create clients")
		return
	}

	for i := 0; i < 2; i++ {

		kwargTaskName := generateUUID()
		kwargTask := &multiplyKwargs{}

		argTaskName := generateUUID()
		argTask := multiply

		for j := 0; j < 2; j++ {
			for i, celeryClient := range celeryClients {
				log.Print(i)
				debugLog(celeryClient, "registering kwarg task %s %p", kwargTaskName, kwargTask)
				celeryClient.Register(kwargTaskName, kwargTask)

				debugLog(celeryClient, "registering arg task %s %p", argTaskName, argTask)
				celeryClient.Register(argTaskName, argTask)

				debugLog(celeryClient, "starting worker")
				celeryClient.StartWorker()

				arg1 := rand.Intn(100)
				arg2 := rand.Intn(100)
				expected := arg1 * arg2

				debugLog(celeryClient, "submitting tasks")
				kwargAsyncResult, err := celeryClient.DelayKwargs(kwargTaskName, map[string]interface{}{
					"a": arg1,
					"b": arg2,
				})
				if err != nil {
					t.Errorf("failed to submit kwarg task %s: %v", kwargTaskName, err)
					return
				}

				argAsyncResult, err := celeryClient.Delay(argTaskName, arg1, arg2)
				if err != nil {
					t.Errorf("failed to submit arg task %s: %v", argTaskName, err)
					return
				}

				debugLog(celeryClient, "waiting for result")
				kwargVal, err := kwargAsyncResult.Get(10 * time.Second)
				if err != nil || kwargVal == nil {
					t.Errorf("failed to get result: %v", err)
					return
				}

				debugLog(celeryClient, "validating result")
				actual := convertInterface(kwargVal)
				if actual != expected {
					t.Errorf("returned result %v is different from expected value %v", actual, expected)
					return
				}

				debugLog(celeryClient, "waiting for result")
				argVal, err := argAsyncResult.Get(10 * time.Second)
				if err != nil {
					t.Errorf("failed to get result: %v", err)
					return
				}

				debugLog(celeryClient, "validating result")
				actual = convertInterface(argVal)
				if actual != expected {
					t.Errorf("returned result %v is different from expected value %v", actual, expected)
					return
				}

				debugLog(celeryClient, "stopping worker")
				celeryClient.StopWorker()
			}
		}
	}

}

func TestRegister(t *testing.T) {
	levelDB, funcC := getLevelDB(t)
	defer funcC()

	celeryClients, err := getClients(levelDB, "test")
	if err != nil {
		t.Errorf("failed to create CeleryClients: %v", err)
		return
	}
	taskName := generateUUID()

	for _, celeryClient := range celeryClients {
		celeryClient.Register(taskName, multiply)
		task := celeryClient.worker.GetTask(taskName)
		if !reflect.DeepEqual(reflect.ValueOf(multiply), reflect.ValueOf(task)) {
			t.Errorf("registered task %v is different from received task %v", reflect.ValueOf(multiply), reflect.ValueOf(task))
			return
		}
	}
}

// RUN this test with -race flag to see if there are race conditions
func TestCeleryWorker_RunTaskTaskCopy(t *testing.T) {
	kwargTaskName := generateUUID()
	kwargTask := &stateFulTask{false, map[string]interface{}{"val": "should not mutate"}}

	inMemoryClient, _ := getInMemoryClient(2)
	inMemoryClient.Register(kwargTaskName, kwargTask)
	inMemoryClient.StartWorker()

	res1, _ := inMemoryClient.DelayKwargs(kwargTaskName, map[string]interface{}{
		"val": generateUUID(),
	})

	res2, _ := inMemoryClient.DelayKwargs(kwargTaskName, map[string]interface{}{
		"val": generateUUID(),
	})

	// following should not cause any race conditions with -race flag
	// because the workers copy the tasks
	res1.Get(10 * time.Second)
	res2.Get(10 * time.Second)

	if kwargTask.state["val"] != "should not mutate" {
		t.Fail()
	}

	inMemoryClient.StopWorker()
}

func TestCeleryWorker_RunTaskTaskCopyError(t *testing.T) {
	kwargTaskName := generateUUID()
	kwargTask := &stateFulTask{true, map[string]interface{}{"val": "should not mutate"}}

	inMemoryClient, _ := getInMemoryClient(2)
	inMemoryClient.Register(kwargTaskName, kwargTask)
	inMemoryClient.StartWorker()

	res1, _ := inMemoryClient.DelayKwargs(kwargTaskName, map[string]interface{}{
		"val": generateUUID(),
	})

	res, _ := res1.Get(1 * time.Millisecond)
	// there should not be any result
	// TODO Update this test when there is a way to store execution errors
	if res != nil {
		t.Fail()
	}
	inMemoryClient.StopWorker()
}

//func TestBlockingGet(t *testing.T) {
//	levelDB, funcC := getLevelDB(t)
//	defer funcC()
//	celeryClients, err := getClients(levelDB, "test")
//	if err != nil {
//		t.Errorf("failed to create CeleryClients: %v", err)
//		return
//	}
//
//	for _, celeryClient := range celeryClients {
//
//		// send task
//		asyncResult, err := celeryClient.Delay("dummy", 3, 5)
//		if err != nil {
//			t.Errorf("failed to get async result")
//			return
//		}
//
//		duration := 1 * time.Second
//		var asyncError error
//
//		go func() {
//			_, asyncError = asyncResult.Get(duration)
//		}()
//
//		time.Sleep(duration + time.Millisecond)
//		if asyncError == nil {
//			t.Errorf("failed to timeout in time")
//			return
//		}
//
//	}
//}

func convertInterface(val interface{}) int {
	f, ok := val.(float64)
	if ok {
		return int(f)
	}
	i, ok := val.(int64)
	if ok {
		return int(i)
	}
	return val.(int)
}
