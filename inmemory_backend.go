package gocelery

import (
	"errors"
	"sync"
)

type inMemoryBackend struct {
	ResultStore map[string]*ResultMessage
	lock        sync.RWMutex
}

// NewInMemoryBackend returns a CeleryBackend implemented InMemory.
func NewInMemoryBackend() CeleryBackend {
	return &inMemoryBackend{make(map[string]*ResultMessage), sync.RWMutex{}}
}

// GetResult returns the result
func (b *inMemoryBackend) GetResult(taskID string) (*ResultMessage, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	if res, ok := b.ResultStore[taskID]; ok {
		return res, nil
	}
	return nil, errors.New("task does not exist")
}

func (b *inMemoryBackend) SetResult(taskID string, res *ResultMessage) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	result := *res
	b.ResultStore[taskID] = &result
	return nil
}

func (b *inMemoryBackend) Clear() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ResultStore = make(map[string]*ResultMessage)
}
