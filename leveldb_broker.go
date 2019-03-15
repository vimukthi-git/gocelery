package gocelery

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// levelDBBroker implements CeleryBroker backed by levelDB
type levelDBBroker struct {
	db    *leveldb.DB
	queue string
	head  uint
	tail  uint

	mu sync.Mutex // protect head and tail
}

// levelDBBrokerState is used to store the Queue state in levelDB
type levelDBBrokerState struct {
	Head uint `json:"head"`
	Tail uint `json:"tail"`
}

// NewLevelDBBroker returns levelDB backed implementation of CeleryBroker
// We will also initialise any previous state
func NewLevelDBBroker(db *leveldb.DB, queue string) CeleryBroker {
	brk := &levelDBBroker{
		db:    db,
		queue: queue,
		head:  0,
		tail:  0,
	}

	// restore the old state from the DB
	brk.loadBrokerState()

	return brk
}

// loadBrokerState loads any previous state if any available
// logs any state retrieval errors
func (lbrk *levelDBBroker) loadBrokerState() {
	key := []byte(fmt.Sprintf("celery-leveldb-broker-state-%s", lbrk.queue))
	data, err := lbrk.db.Get(key, nil)
	if err != nil {
		return
	}

	state := new(levelDBBrokerState)
	err = json.Unmarshal(data, state)
	if err != nil {
		log.Errorf("failed to load the previous state from levelDB: %v\n", err)
		return
	}

	lbrk.head = state.Head
	lbrk.tail = state.Tail
}

// saveBrokerState saves the broker state to the disk
// due to limitation of worker, we shall save state on every update
// to ensure the state is upto date on restart
func (lbrk *levelDBBroker) saveBrokerState() error {
	state := levelDBBrokerState{Head: lbrk.head, Tail: lbrk.tail}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	key := []byte(fmt.Sprintf("celery-leveldb-broker-state-%s", lbrk.queue))
	err = lbrk.db.Put(key, data, nil)
	if err != nil {
		return err
	}

	return nil
}

// count returns the current items in the queue
func (lbrk *levelDBBroker) count() uint {
	c := lbrk.tail - lbrk.head
	if c < 0 {
		c = 0
	}

	return c
}

// GetTaskMessage returns the first message in the levelDB queue
func (lbrk *levelDBBroker) GetTaskMessage() (*TaskMessage, error) {
	lbrk.mu.Lock()
	defer lbrk.mu.Unlock()

	if lbrk.count() == 0 {
		return nil, nil
	}

	// get the task
	key := []byte(fmt.Sprintf("celery-leveldb-task-%v", lbrk.head))
	data, err := lbrk.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	// increment the head
	lbrk.head++

	// if the head == tail, start from 0
	if lbrk.head == lbrk.tail {
		lbrk.head = 0
		lbrk.tail = 0
	}

	// save broker state
	err = lbrk.saveBrokerState()
	if err != nil {
		return nil, fmt.Errorf("failed to save broker state: %v", err)
	}

	// delete the task
	err = lbrk.db.Delete(key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to delete the task: %v", err)
	}

	// load the task
	msg := new(TaskMessage)
	err = json.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// SendCeleryMessage enqueues the messages to the levelDB queue
func (lbrk *levelDBBroker) SendCeleryMessage(msg *CeleryMessage) (err error) {
	lbrk.mu.Lock()
	defer lbrk.mu.Unlock()

	if msg == nil {
		return fmt.Errorf("empty message")
	}

	tskMsg := msg.GetTaskMessage()
	if tskMsg == nil {
		return fmt.Errorf("failed to get task message")
	}

	// generate key and save task
	var data []byte
	data, err = json.Marshal(tskMsg)
	if err != nil {
		return err
	}

	key := []byte(fmt.Sprintf("celery-leveldb-task-%v", lbrk.tail))
	defer func() {
		// if there was an error, reset the tail
		if err != nil {
			lbrk.tail--
			lbrk.saveBrokerState()
		}
	}()

	// increment the tail
	lbrk.tail++

	// save state
	err = lbrk.saveBrokerState()
	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	err = lbrk.db.Put(key, data, nil)
	if err != nil {
		return err
	}

	return nil
}
