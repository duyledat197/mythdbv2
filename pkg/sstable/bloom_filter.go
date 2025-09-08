package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bloom/v3"
)

// writeBloomFilter writes a bloom filter to the writer
func writeBloomFilter(writer *bufio.Writer, bloomFilter *bloom.BloomFilter) error {
	// Serialize bloom filter to bytes
	bloomData, err := bloomFilter.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal bloom filter: %w", err)
	}

	// Write bloom filter length
	length := uint32(len(bloomData))
	if err := binary.Write(writer, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write bloom filter length: %w", err)
	}

	// Write bloom filter data
	if _, err := writer.Write(bloomData); err != nil {
		return fmt.Errorf("failed to write bloom filter data: %w", err)
	}

	return nil
}

// readBloomFilter reads a bloom filter from the reader
func readBloomFilterFromReader(reader *bufio.Reader) (*bloom.BloomFilter, error) {
	// Read bloom filter length
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter length: %w", err)
	}

	// Read bloom filter data
	bloomData := make([]byte, length)
	if _, err := io.ReadFull(reader, bloomData); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter data: %w", err)
	}

	// Deserialize bloom filter
	bloomFilter := &bloom.BloomFilter{}
	if err := bloomFilter.UnmarshalBinary(bloomData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bloom filter: %w", err)
	}

	return bloomFilter, nil
}
