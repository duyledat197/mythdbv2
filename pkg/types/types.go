package types

import "time"

type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone bool
}

type RecordValue struct {
	Value     []byte
	Tombstone bool
}

// NewEntry creates a new entry with current timestamp
func NewEntry(key []byte, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Tombstone: false,
	}
}

// NewDeleteEntry creates a delete marker entry
func NewDeleteEntry(key []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
		Tombstone: true,
	}
}
