package rrdb

import (
	"sync"

	"github.com/renproject/kv/db"
)

// rrdb is an in-memory implementation of the `db.Iterable`. rrdb uses the
// Random Replacement policy to remove data when it runs out of storage space.
type rrdb struct {
	cap  int
	mu   *sync.RWMutex
	data map[string][]byte
}

// New returns a new rrdb.
func New(cap int) db.Iterable {
	return &rrdb{
		cap:  cap,
		mu:   new(sync.RWMutex),
		data: map[string][]byte{},
	}
}

// Insert implements the `db.Iterable` interface.
func (rrdb rrdb) Insert(key string, value []byte) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	rrdb.mu.Lock()
	defer rrdb.mu.Unlock()

	if len(rrdb.data) >= rrdb.cap {
		for deleteKey := range rrdb.data {
			delete(rrdb.data, deleteKey)
			break
		}
	}
	rrdb.data[key] = value
	return nil
}

// Get implements the `db.Iterable` interface.
func (rrdb rrdb) Get(key string) ([]byte, error) {
	rrdb.mu.RLock()
	defer rrdb.mu.RUnlock()

	val, ok := rrdb.data[key]
	if !ok {
		return nil, db.ErrNotFound
	}
	return val, nil
}

// Delete implements the `db.Iterable` interface.
func (rrdb rrdb) Delete(key string) error {
	rrdb.mu.Lock()
	defer rrdb.mu.Unlock()

	delete(rrdb.data, key)
	return nil
}

// Size implements the `db.Iterable` interface.
func (rrdb rrdb) Size() (int, error) {
	rrdb.mu.RLock()
	defer rrdb.mu.RUnlock()

	return len(rrdb.data), nil
}

// Iterator implements the `db.Iterable` interface.
func (rrdb rrdb) Iterator() db.Iterator {
	rrdb.mu.RLock()
	defer rrdb.mu.RUnlock()

	return newIterator(rrdb.data)
}

type iterator struct {
	index  int
	keys   []string
	values [][]byte
}

func newIterator(data map[string][]byte) db.Iterator {
	keys := make([]string, 0, len(data))
	values := make([][]byte, 0, len(data))
	for key, value := range data {
		keys = append(keys, key)
		values = append(values, value)
	}

	return &iterator{
		index:  -1,
		keys:   keys,
		values: values,
	}
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	iter.index++
	return iter.index < len(iter.keys)
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	if iter.index == -1 {
		return "", db.ErrIndexOutOfRange
	}
	if iter.index >= len(iter.keys) {
		return "", db.ErrIndexOutOfRange
	}
	return iter.keys[iter.index], nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value() ([]byte, error) {
	if iter.index == -1 {
		return nil, db.ErrIndexOutOfRange
	}
	if iter.index >= len(iter.keys) {
		return nil, db.ErrIndexOutOfRange
	}
	return iter.values[iter.index], nil
}
