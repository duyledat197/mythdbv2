package manifest

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"mythdb/pkg/sstable"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Manifest represents a manifest file interface
type Manifest interface {
	AddSSTable(level int, sstable sstable.SSTable) error
	RemoveSSTable(level int, sstable sstable.SSTable) error
	GetSSTables(level int) []sstable.SSTable
	GetLevels() []int
	Save() error
	Load() error
	Close() error
}

// ManifestEntry represents an entry in the manifest
type ManifestEntry struct {
	Level  int
	Path   string
	MinKey string
	MaxKey string
	Size   int64
}

// manifest implements the manifest interface
type manifest struct {
	path    string
	entries map[int][]ManifestEntry
	mu      sync.RWMutex
	closed  bool
}

// NewManifest creates a new manifest
func NewManifest(path string) (*manifest, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create manifest directory: %w", err)
	}

	manifest := &manifest{
		path:    path,
		entries: make(map[int][]ManifestEntry),
		closed:  false,
	}

	// Try to load existing manifest
	if err := manifest.Load(); err != nil {
		// If file doesn't exist, that's okay - we'll create a new one
		if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no such file or directory") {
			return nil, fmt.Errorf("failed to load existing manifest: %w", err)
		}
		// Initialize empty entries map for new manifest
		manifest.entries = make(map[int][]ManifestEntry)
	}

	return manifest, nil
}

// AddSSTable adds an SSTable to the manifest
func (m *manifest) AddSSTable(level int, table sstable.SSTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	// Get SSTable path (assuming it's a file-based SSTable)
	path := table.Path()

	entry := ManifestEntry{
		Level:  level,
		Path:   path,
		MinKey: string(table.MinKey()),
		MaxKey: string(table.MaxKey()),
		Size:   table.Size(),
	}

	m.entries[level] = append(m.entries[level], entry)

	// Sort entries by min key for each level
	sort.Slice(m.entries[level], func(i, j int) bool {
		return m.entries[level][i].MinKey < m.entries[level][j].MinKey
	})

	return m.Save()
}

// RemoveSSTable removes an SSTable from the manifest
func (m *manifest) RemoveSSTable(level int, table sstable.SSTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	// Get SSTable path
	path := table.Path()
	// Find and remove the entry
	entries := m.entries[level]
	for i, entry := range entries {
		if entry.Path == path {
			m.entries[level] = append(entries[:i], entries[i+1:]...)
			return m.Save()
		}
	}

	return fmt.Errorf("SSTable not found in manifest")
}

// GetSSTables returns all SSTables for a given level
func (m *manifest) GetSSTables(level int) []sstable.SSTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := m.entries[level]
	sstables := make([]sstable.SSTable, 0, len(entries))

	for _, entry := range entries {
		sstable, err := sstable.OpenSSTable(entry.Path)
		if err != nil {
			// Log error but continue
			continue
		}
		sstables = append(sstables, sstable)
	}

	return sstables
}

// GetLevels returns all levels that have SSTables
func (m *manifest) GetLevels() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	levels := make([]int, 0, len(m.entries))
	for level := range m.entries {
		if len(m.entries[level]) > 0 {
			levels = append(levels, level)
		}
	}

	sort.Ints(levels)
	return levels
}

type onDiskManifest struct {
	Entries  map[int][]ManifestEntry
	Checksum uint32
}

// Save saves the manifest to disk
func (m *manifest) Save() error {
	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	// Compute checksum over gob-encoded entries
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m.entries); err != nil {
		return fmt.Errorf("failed to gob-encode manifest entries: %w", err)
	}
	checksum := crc32.ChecksumIEEE(buf.Bytes())

	// Prepare on-disk struct
	disk := onDiskManifest{
		Entries:  m.entries,
		Checksum: checksum,
	}

	// Write to temporary file
	tempPath := m.path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary manifest: %w", err)
	}
	defer file.Close()

	fileEnc := gob.NewEncoder(file)
	if err := fileEnc.Encode(&disk); err != nil {
		return fmt.Errorf("failed to gob-encode manifest to file: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync manifest: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temporary manifest: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, m.path); err != nil {
		return fmt.Errorf("failed to rename temporary manifest: %w", err)
	}

	return nil
}

// Load loads the manifest from disk
func (m *manifest) Load() error {
	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	file, err := os.Open(m.path)
	if err != nil {
		return fmt.Errorf("failed to open manifest: %w", err)
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	var disk onDiskManifest
	if err := dec.Decode(&disk); err != nil {
		return fmt.Errorf("failed to gob-decode manifest: %w", err)
	}

	// Verify checksum
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(disk.Entries); err != nil {
		return fmt.Errorf("failed to gob-encode entries for checksum: %w", err)
	}
	actual := crc32.ChecksumIEEE(buf.Bytes())
	if actual != disk.Checksum {
		return fmt.Errorf("manifest checksum mismatch: expected %x, got %x", disk.Checksum, actual)
	}

	m.entries = disk.Entries
	return nil
}

// Close closes the manifest
func (m *manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	return nil
}

// GetEntries returns all manifest entries (for debugging/testing)
func (m *manifest) GetEntries() map[int][]ManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy
	result := make(map[int][]ManifestEntry)
	for level, entries := range m.entries {
		result[level] = make([]ManifestEntry, len(entries))
		copy(result[level], entries)
	}
	return result
}

// GetSSTableCount returns the number of SSTables in each level
func (m *manifest) GetSSTableCount() map[int]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[int]int)
	for level, entries := range m.entries {
		result[level] = len(entries)
	}
	return result
}

// GetTotalSize returns the total size of all SSTables
func (m *manifest) GetTotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalSize int64
	for _, entries := range m.entries {
		for _, entry := range entries {
			totalSize += entry.Size
		}
	}
	return totalSize
}

// GetLevelSize returns the total size of SSTables in a specific level
func (m *manifest) GetLevelSize(level int) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var size int64
	for _, entry := range m.entries[level] {
		size += entry.Size
	}
	return size
}

// FindSSTablesForRange finds SSTables that might contain keys in the given range
func (m *manifest) FindSSTablesForRange(level int, minKey, maxKey []byte) []ManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []ManifestEntry
	entries := m.entries[level]

	for _, entry := range entries {
		// Check if there's any overlap
		if string(maxKey) >= entry.MinKey && string(minKey) <= entry.MaxKey {
			result = append(result, entry)
		}
	}

	return result
}

// GetManifestPath returns the manifest file path
func (m *manifest) GetManifestPath() string {
	return m.path
}
