package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"mythdb/pkg/types"
	"os"
)

// readEntry reads an entry from the file
func (s *ssTable) readEntry(indexEntry IndexEntry) (*types.Entry, error) {
	if s.file == nil {
		// Reopen file if needed
		file, err := os.Open(s.path)
		if err != nil {
			return nil, fmt.Errorf("failed to reopen SSTable file: %w", err)
		}
		s.file = file
	}

	// Seek to entry position
	if _, err := s.file.Seek(indexEntry.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to entry: %w", err)
	}

	// Read CRC32 checksum
	var expectedChecksum uint32
	if err := binary.Read(s.file, binary.BigEndian, &expectedChecksum); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}

	// Read entry data
	data := make([]byte, indexEntry.Length)
	if _, err := io.ReadFull(s.file, data); err != nil {
		return nil, fmt.Errorf("failed to read entry data: %w", err)
	}

	// Verify CRC32 checksum
	actualChecksum := crc32.ChecksumIEEE(data)
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %x, got %x", expectedChecksum, actualChecksum)
	}

	// Deserialize entry
	entry, err := deserializeEntry(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize entry: %w", err)
	}

	return entry, nil
}

// serializeEntry serializes an entry to bytes (same format as WAL)
func serializeEntry(entry *types.Entry) ([]byte, error) {
	keyLen := len(entry.Key)
	valueLen := len(entry.Value)

	data := make([]byte, 4+keyLen+4+valueLen+8+1)
	offset := 0

	// Key length
	binary.BigEndian.PutUint32(data[offset:], uint32(keyLen))
	offset += 4

	// Key
	copy(data[offset:], entry.Key)
	offset += keyLen

	// Value length
	binary.BigEndian.PutUint32(data[offset:], uint32(valueLen))
	offset += 4

	// Value
	copy(data[offset:], entry.Value)
	offset += valueLen

	// Timestamp
	binary.BigEndian.PutUint64(data[offset:], uint64(entry.Timestamp))
	offset += 8

	// Deleted flag
	if entry.Tombstone {
		data[offset] = 1
	} else {
		data[offset] = 0
	}

	return data, nil
}

// deserializeEntry deserializes bytes to an entry (same format as WAL)
func deserializeEntry(data []byte) (*types.Entry, error) {
	offset := 0

	// Key length
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid data: too short for key length")
	}
	keyLen := binary.BigEndian.Uint32(data[offset:])
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
	valueLen := binary.BigEndian.Uint32(data[offset:])
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
	timestamp := int64(binary.BigEndian.Uint64(data[offset:]))
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
