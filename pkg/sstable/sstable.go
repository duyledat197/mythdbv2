package sstable

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"mythdb/pkg/iterator"
	"mythdb/pkg/types"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

type SSTable interface {
	Get(key []byte) (*types.Entry, error)
	Iterator() iterator.Iterator[*types.Entry]
	Size() int64
	MinKey() []byte
	MaxKey() []byte
	Path() string
	Close() error
}

// SSTable implements the SSTable interface
type ssTable struct {
	path        string
	file        *os.File
	index       []IndexEntry
	minKey      []byte
	maxKey      []byte
	size        int64
	bloomFilter *bloom.BloomFilter
	mu          sync.RWMutex
	closed      bool
}

// IndexEntry represents an entry in the SSTable index
type IndexEntry struct {
	Key            []byte
	Offset         int64
	Length         int32
	ChecksumOffset int64
}

// NewSSTable creates a new SSTable from linked list of entries
func NewSSTable(path string, entriesList *list.List) (SSTable, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create SSTable directory: %w", err)
	}

	if entriesList == nil || entriesList.Len() == 0 {
		return nil, fmt.Errorf("no entries to create SSTable")
	}

	// Entries are already sorted from priority queue merging, no need to sort again

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}

	writer := bufio.NewWriter(file)
	var index []IndexEntry
	var minKey, maxKey []byte
	var totalSize int64

	// Create Bloom Filter
	// Estimate optimal parameters: n = number of entries, p = false positive rate (0.01 = 1%)
	estimatedEntries := uint(entriesList.Len())
	if estimatedEntries == 0 {
		estimatedEntries = 1 // Avoid zero
	}
	bloomFilter := bloom.NewWithEstimates(estimatedEntries, 0.01)

	// Write entries and build index directly from linked list
	entryIndex := 0
	for e := entriesList.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*types.Entry)
		// Serialize entry
		data, err := serializeEntry(entry)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to serialize entry: %w", err)
		}

		// Calculate CRC32 checksum
		checksum := crc32.ChecksumIEEE(data)

		// Write checksum
		if err := binary.Write(writer, binary.BigEndian, checksum); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write checksum: %w", err)
		}

		// Write data
		length := int32(len(data))
		if _, err := writer.Write(data); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write entry: %w", err)
		}

		// Flush writer to get accurate offset
		if err := writer.Flush(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to flush writer: %w", err)
		}

		// Get current offset after writing
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to get file offset: %w", err)
		}

		// Add to index
		index = append(index, IndexEntry{
			Key:            entry.Key,
			Offset:         offset - int64(4+length), // Start of entry (before checksum)
			Length:         length,
			ChecksumOffset: offset - int64(4+length), // Checksum is at the beginning
		})

		// Add key to Bloom Filter
		bloomFilter.Add(entry.Key)

		// Update min/max keys
		if minKey == nil || bytes.Compare(entry.Key, minKey) < 0 {
			minKey = entry.Key
		}
		if maxKey == nil || bytes.Compare(entry.Key, maxKey) > 0 {
			maxKey = entry.Key
		}

		totalSize += int64(length)

		entryIndex++
	}

	// Write Bloom Filter at the end
	bloomOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get bloom filter offset: %w", err)
	}

	if err := writeBloomFilter(writer, bloomFilter); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write bloom filter: %w", err)
	}

	// Write index at the end
	indexOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get index offset: %w", err)
	}

	if err := writeIndex(writer, index); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write index: %w", err)
	}

	// Write bloom filter offset
	if err := binary.Write(writer, binary.BigEndian, bloomOffset); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write bloom filter offset: %w", err)
	}

	// Write index offset
	if err := binary.Write(writer, binary.BigEndian, indexOffset); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write index offset: %w", err)
	}

	if err := writer.Flush(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to flush SSTable: %w", err)
	}

	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close SSTable file: %w", err)
	}

	return &ssTable{
		path:        path,
		index:       index,
		minKey:      minKey,
		maxKey:      maxKey,
		size:        totalSize,
		bloomFilter: bloomFilter,
		closed:      false,
	}, nil
}

// OpenSSTable opens an existing SSTable
func OpenSSTable(path string) (SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	// Read bloom filter and index offsets from end of file
	if _, err := file.Seek(-16, io.SeekEnd); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to offsets: %w", err)
	}

	var bloomOffset int64
	if err := binary.Read(file, binary.BigEndian, &bloomOffset); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read bloom filter offset: %w", err)
	}

	var indexOffset int64
	if err := binary.Read(file, binary.BigEndian, &indexOffset); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read index offset: %w", err)
	}

	// Read bloom filter
	if _, err := file.Seek(bloomOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to bloom filter: %w", err)
	}

	reader := bufio.NewReader(file)
	bloomFilter, err := readBloomFilterFromReader(reader)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	// Read index
	if _, err := file.Seek(indexOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to index: %w", err)
	}

	// Recreate reader after seek to avoid stale buffer
	reader = bufio.NewReader(file)
	index, err := readIndex(reader)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	// Determine min/max keys
	var minKey, maxKey []byte
	if len(index) > 0 {
		minKey = index[0].Key
		maxKey = index[len(index)-1].Key
	}

	// Calculate total size
	var totalSize int64
	for _, entry := range index {
		totalSize += int64(entry.Length)
	}

	return &ssTable{
		path:        path,
		file:        file,
		index:       index,
		minKey:      minKey,
		maxKey:      maxKey,
		size:        totalSize,
		bloomFilter: bloomFilter,
		closed:      false,
	}, nil
}

// Get retrieves a value by key
func (s *ssTable) Get(key []byte) (*types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("SSTable is closed")
	}

	// Check Bloom Filter first - if key is not in filter, it's definitely not in SSTable
	if s.bloomFilter != nil && !s.bloomFilter.Test(key) {
		return nil, nil // Key definitely not found
	}

	// Binary search in index
	idx := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].Key, key) >= 0
	})

	if idx >= len(s.index) || !bytes.Equal(s.index[idx].Key, key) {
		return nil, nil // Key not found
	}

	// Read entry from file
	entry, err := s.readEntry(s.index[idx])
	if err != nil {
		return nil, fmt.Errorf("failed to read entry: %w", err)
	}

	return entry, nil
}

// Iterator returns an iterator for the SSTable
func (s *ssTable) Iterator() iterator.Iterator[*types.Entry] {
	return &Iterator{
		sstable: s,
		index:   0,
	}
}

// Size returns the size of the SSTable
func (s *ssTable) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// MinKey returns the minimum key in the SSTable
func (s *ssTable) MinKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.minKey
}

// MaxKey returns the maximum key in the SSTable
func (s *ssTable) MaxKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxKey
}

// Close closes the SSTable
func (s *ssTable) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable file: %w", err)
		}
	}

	s.closed = true
	return nil
}

// Path returns the file path of the SSTable
func (s *ssTable) Path() string {
	return s.path
}
