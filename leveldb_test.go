package gocelery

import (
	cr "crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func getRandomTestStoragePath(t *testing.T) string {
	r := make([]byte, 32)
	n, err := cr.Read(r)
	assert.Nil(t, err)
	assert.Equal(t, n, 32)
	return fmt.Sprintf("%s_%x", "/tmp/centrifuge_data.leveldb_TESTING", r)
}

func getLevelDB(t *testing.T) (*leveldb.DB, func()) {
	path := getRandomTestStoragePath(t)
	levelDB, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("failed to open levelDb: %v", err)
	}

	return levelDB, func() {
		levelDB.Close()
		os.Remove(path)
	}
}
func TestLevelDBBroker_SaveLoadState(t *testing.T) {
	path := getRandomTestStoragePath(t)
	db, err := leveldb.OpenFile(path, nil)
	assert.Nil(t, err)
	defer func() {
		db.Close()
		os.Remove(path)
	}()

	queue := "test1"
	// no initial state
	brk := NewLevelDBBroker(db, queue)
	assert.NotNil(t, brk)
	lbrk := brk.(*levelDBBroker)
	assert.Equal(t, lbrk.head, uint(0))
	assert.Equal(t, lbrk.tail, uint(0))

	lbrk.head = 10
	lbrk.tail = 30
	err = lbrk.saveBrokerState()
	assert.Nil(t, err)

	// load state
	brk = NewLevelDBBroker(db, queue)
	assert.NotNil(t, brk)
	lbrk = brk.(*levelDBBroker)
	assert.Equal(t, lbrk.head, uint(10))
	assert.Equal(t, lbrk.tail, uint(30))

	// new state for new queue
	queue = "test2"
	brk = NewLevelDBBroker(db, queue)
	assert.NotNil(t, brk)
	lbrk = brk.(*levelDBBroker)
	assert.Equal(t, lbrk.head, uint(0))
	assert.Equal(t, lbrk.tail, uint(0))

}
