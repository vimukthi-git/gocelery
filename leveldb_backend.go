package gocelery

import (
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

// levelDBBackend implements the CeleryBackend based on levelDB
type levelDBBackend struct {
	db *leveldb.DB
}

// NewLevelDBBackend returns an levelDB implementation of CeleryBackend
func NewLevelDBBackend(db *leveldb.DB) CeleryBackend {
	return levelDBBackend{db: db}
}

// GetResult returns the result stored at key
func (ldb levelDBBackend) GetResult(key string) (*ResultMessage, error) {
	data, err := ldb.db.Get([]byte(fmt.Sprintf("celery-leveldb-result-%s", key)), nil)
	if err != nil {
		return nil, err
	}

	res := new(ResultMessage)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	return res, nil
}

// SetResult stores the result at key
func (ldb levelDBBackend) SetResult(key string, result *ResultMessage) error {
	if result == nil {
		return fmt.Errorf("empty result")
	}

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshall result: %v", err)
	}

	return ldb.db.Put([]byte(fmt.Sprintf("celery-leveldb-result-%s", key)), data, nil)
}
