package sstable

import (
	"bytes"
	"mythdb/pkg/iterator"
	"mythdb/pkg/types"
	"sort"
)

// Iterator implements Iterator
type Iterator struct {
	sstable *ssTable
	index   int
	started bool
}

// Next moves the iterator to the next entry
func (iter *Iterator) Next() bool {
	if !iter.started {
		iter.started = true
		return iter.index < len(iter.sstable.index)
	}
	iter.index++
	return iter.index < len(iter.sstable.index)
}

// Entry returns the current entry
func (iter *Iterator) Entry() *types.Entry {
	if iter.index >= len(iter.sstable.index) {
		return nil
	}

	entry, err := iter.sstable.readEntry(iter.sstable.index[iter.index])
	if err != nil {
		return nil
	}

	return entry
}

// Seek moves the iterator to the first entry with key >= target
func (iter *Iterator) Seek(key []byte) bool {
	iter.index = sort.Search(len(iter.sstable.index), func(i int) bool {
		return bytes.Compare(iter.sstable.index[i].Key, key) >= 0
	})

	return iter.index < len(iter.sstable.index)
}

// Close closes the iterator
func (iter *Iterator) Close() error {
	iter.index = 0
	return nil
}

// SSTableIteratorItem represents an item in the priority queue for merging SSTables
type IteratorItem struct {
	Entry    *types.Entry
	Iterator iterator.Iterator[*types.Entry]
	Index    int // SSTable index for tie-breaking
}
