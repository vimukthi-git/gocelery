package gocelery

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// redisCeleryBackend is CeleryBackend for Redis
type redisCeleryBackend struct {
	*redis.Pool
}

// NewRedisCeleryBackend creates new redisCeleryBackend
func NewRedisCeleryBackend(uri string) CeleryBackend {
	return &redisCeleryBackend{Pool: newRedisPool(uri)}
}

// GetResult calls API to get asynchronous result
// Should be called by AsyncResult
func (cb *redisCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	//"celery-task-meta-" + taskID
	conn := cb.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}

	result := new(ResultMessage)
	err = json.Unmarshal(val.([]byte), result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// SetResult pushes result back into backend
func (cb *redisCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, resBytes)
	return err
}
