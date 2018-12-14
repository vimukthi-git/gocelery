package gocelery

import (
	"sync"
)

type inMemoryBroker struct {
	taskQueue []*TaskMessage
	lock      sync.RWMutex
}

// NewInMemoryBroker returns immeory backed CeleryBroker
func NewInMemoryBroker() CeleryBroker {
	return &inMemoryBroker{make([]*TaskMessage, 0), sync.RWMutex{}}
}

func (b *inMemoryBroker) SendCeleryMessage(m *CeleryMessage) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.taskQueue = append(b.taskQueue, m.GetTaskMessage())
	return nil
}

func (b *inMemoryBroker) GetTaskMessage() (t *TaskMessage, e error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.taskQueue) == 0 {
		return nil, nil
	}
	t, b.taskQueue = b.taskQueue[0], b.taskQueue[1:]
	return t, nil
}

func (b *inMemoryBroker) Clear() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.taskQueue = make([]*TaskMessage, 0)
	return nil
}

func (b *inMemoryBroker) isEmpty() bool {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return len(b.taskQueue) == 0
}
