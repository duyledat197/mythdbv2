package memtable

import (
	"mythdb/pkg/types"

	"github.com/huandu/skiplist"
)

// Iterator implements MemTableIterator
type Iterator struct {
	memtable *memTable
	current  *skiplist.Element
	started  bool
}

// Next moves the iterator to the next entry
func (iter *Iterator) Next() bool {
	if !iter.started {
		iter.started = true
		return iter.current != nil
	}
	if iter.current == nil {
		return false
	}
	iter.current = iter.current.Next()
	return iter.current != nil
}

// Entry returns the current entry
func (iter *Iterator) Entry() *types.Entry {
	if iter.current == nil {
		return nil
	}

	entry, ok := iter.current.Value.(*types.Entry)
	if !ok {
		return nil
	}

	return entry
}

// Close closes the iterator
func (iter *Iterator) Close() error {
	iter.current = nil
	return nil
}
