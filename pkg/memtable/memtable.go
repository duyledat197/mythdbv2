package memtable

import (
	"sync"

	"github.com/huandu/skiplist"

	"mythdb/pkg/iterator"
	"mythdb/pkg/types"
)

type MemTable interface {
	Put(key []byte, value []byte) error
	Get(key []byte) (*types.Entry, error)
	Delete(key []byte) error
	Iterator() iterator.Iterator[*types.Entry]
	Size() int
	IsFull() bool
	Clear() error
}

// memTable implements MemTable using huandu/skiplist
type memTable struct {
	skiplist *skiplist.SkipList
	maxSize  int
	mu       sync.RWMutex
}

// NewSkipListMemTable creates a new skip list memtable
func NewMemTable(maxSize int) MemTable {
	// Create skiplist with string comparison
	sl := skiplist.New(skiplist.Bytes)

	return &memTable{
		skiplist: sl,
		maxSize:  maxSize,
	}
}

// Put inserts or updates a key-value pair
func (sl *memTable) Put(key []byte, value []byte) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := types.NewEntry(key, value)

	// Check if key already exists
	sl.skiplist.Set(key, entry)

	return nil
}

// Get retrieves a value by key
func (sl *memTable) Get(key []byte) (*types.Entry, error) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	elem := sl.skiplist.Get(key)

	if elem == nil {
		return nil, nil
	}

	entry, ok := elem.Value.(*types.Entry)
	if !ok {
		return nil, nil
	}

	return entry, nil
}

// Delete marks a key as deleted
func (sl *memTable) Delete(key []byte) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	deleteEntry := types.NewDeleteEntry(key)
	sl.skiplist.Set(key, deleteEntry)

	return nil
}

// Size returns the number of entries in the memtable
func (sl *memTable) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.skiplist.Len()
}

// IsFull checks if the memtable has reached its maximum size
func (sl *memTable) IsFull() bool {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.skiplist.Len() >= sl.maxSize
}

// Clear removes all entries from the memtable
func (sl *memTable) Clear() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Create new skiplist
	sl.skiplist = skiplist.New(skiplist.Bytes)
	return nil
}

// Iterator returns an iterator for the memtable
func (sl *memTable) Iterator() iterator.Iterator[*types.Entry] {
	return &Iterator{
		memtable: sl,
		current:  sl.skiplist.Front(),
	}
}
