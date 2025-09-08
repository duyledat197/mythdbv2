package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"mythdb/pkg/types"
	"os"
	"path/filepath"
	"sync"
)

// WAL represents a write-ahead log interface
type WAL interface {
	Write(entry *types.Entry) error
	Read() ([]*types.Entry, error)
	Clear() error
	Close() error
}

// wal implements the Write-Ahead Log interface
type wal struct {
	file   *os.File
	writer *bufio.Writer
	reader *bufio.Reader
	path   string
	mu     sync.RWMutex
	closed bool
}

// NewWAL creates a new WAL instance
func NewWAL(path string) (*wal, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &wal{
		file:   file,
		writer: bufio.NewWriter(file),
		reader: bufio.NewReader(file),
		path:   path,
		closed: false,
	}, nil
}

// Write writes an entry to the WAL
func (w *wal) Write(entry *types.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Serialize entry
	data, err := w.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Calculate CRC32 checksum
	checksum := crc32.ChecksumIEEE(data)

	// Write length prefix
	length := uint32(len(data))
	if err := binary.Write(w.writer, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write CRC32 checksum
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	// Write data
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Flush to ensure durability
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// Read reads all entries from the WAL
func (w *wal) Read() ([]*types.Entry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed {
		return nil, fmt.Errorf("WAL is closed")
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	var entries []*types.Entry

	for {
		// Read length prefix
		var length uint32
		if err := binary.Read(w.reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		// Read CRC32 checksum
		var expectedChecksum uint32
		if err := binary.Read(w.reader, binary.LittleEndian, &expectedChecksum); err != nil {
			return nil, fmt.Errorf("failed to read checksum: %w", err)
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(w.reader, data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}

		// Verify CRC32 checksum
		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			return nil, fmt.Errorf("checksum mismatch: expected %x, got %x", expectedChecksum, actualChecksum)
		}

		// Deserialize entry
		entry, err := w.deserializeEntry(data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize entry: %w", err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Clear truncates the WAL file
func (w *wal) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Close current file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush before clear: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close file before clear: %w", err)
	}

	// Truncate file
	if err := os.Truncate(w.path, 0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// Reopen file
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	w.reader = bufio.NewReader(file)

	return nil
}

// Close closes the WAL
func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush on close: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	w.closed = true
	return nil
}

// serializeEntry serializes an entry to bytes
func (w *wal) serializeEntry(entry *types.Entry) ([]byte, error) {
	// Simple serialization format:
	// [key_length:4][key:key_length][value_length:4][value:value_length][timestamp:8][deleted:1]

	keyLen := len(entry.Key)
	valueLen := len(entry.Value)

	data := make([]byte, 4+keyLen+4+valueLen+8+1)
	offset := 0

	// Key length
	binary.LittleEndian.PutUint32(data[offset:], uint32(keyLen))
	offset += 4

	// Key
	copy(data[offset:], entry.Key)
	offset += keyLen

	// Value length
	binary.LittleEndian.PutUint32(data[offset:], uint32(valueLen))
	offset += 4

	// Value
	copy(data[offset:], entry.Value)
	offset += valueLen

	// Timestamp
	binary.LittleEndian.PutUint64(data[offset:], uint64(entry.Timestamp))
	offset += 8

	// Deleted flag
	if entry.Tombstone {
		data[offset] = 1
	} else {
		data[offset] = 0
	}

	return data, nil
}

// deserializeEntry deserializes bytes to an entry
func (w *wal) deserializeEntry(data []byte) (*types.Entry, error) {
	offset := 0

	// Key length
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid data: too short for key length")
	}
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Key
	if len(data) < offset+int(keyLen) {
		return nil, fmt.Errorf("invalid data: too short for key")
	}
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Value length
	if len(data) < offset+4 {
		return nil, fmt.Errorf("invalid data: too short for value length")
	}
	valueLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Value
	if len(data) < offset+int(valueLen) {
		return nil, fmt.Errorf("invalid data: too short for value")
	}
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+int(valueLen)])
	offset += int(valueLen)

	// Timestamp
	if len(data) < offset+8 {
		return nil, fmt.Errorf("invalid data: too short for timestamp")
	}
	timestamp := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Deleted flag
	if len(data) < offset+1 {
		return nil, fmt.Errorf("invalid data: too short for deleted flag")
	}
	deleted := data[offset] == 1

	return &types.Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: deleted,
	}, nil
}
