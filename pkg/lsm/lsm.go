package lsm

import (
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"mythdb/manifest"
	"mythdb/pkg/iterator"
	"mythdb/pkg/memtable"
	priorityqueue "mythdb/pkg/priority_queue"
	"mythdb/pkg/sstable"
	"mythdb/pkg/types"
	"mythdb/pkg/wal"
)

// Config holds LSM tree configuration
type Config struct {
	DataDir             string
	MaxMemTableSize     int
	MaxLevels           int
	LevelSizeMultiplier int
	CompactionThreshold int
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		DataDir:             "./data",
		MaxMemTableSize:     1000,
		MaxLevels:           7,
		LevelSizeMultiplier: 10,
		CompactionThreshold: 2,
	}
}

// LSM implements the LSM tree
type LSM struct {
	config   *Config
	memtable memtable.MemTable
	wal      wal.WAL
	manifest manifest.Manifest
	mu       sync.RWMutex
	closed   bool
}

// NewLSM creates a new LSM tree
func NewLSM(config *Config) (*LSM, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Ensure data directory exists
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create WAL
	walPath := filepath.Join(config.DataDir, "wal.log")
	wal, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Create manifest
	manifestPath := filepath.Join(config.DataDir, "manifest.json")
	manifest, err := manifest.NewManifest(manifestPath)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to create manifest: %w", err)
	}

	// Create memtable
	memtable := memtable.NewMemTable(config.MaxMemTableSize)

	lsm := &LSM{
		config:   config,
		memtable: memtable,
		wal:      wal,
		manifest: manifest,
		closed:   false,
	}

	// Recover from WAL if it exists
	if err := lsm.recoverFromWAL(); err != nil {
		lsm.Close()
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return lsm, nil
}

// Put inserts or updates a key-value pair
func (l *LSM) Put(ctx context.Context, key []byte, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("LSM tree is closed")
	}

	entry := types.NewEntry(key, value)

	// Write to WAL first
	if err := l.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Write to memtable
	if err := l.memtable.Put(key, value); err != nil {
		return fmt.Errorf("failed to write to memtable: %w", err)
	}

	// Check if memtable is full
	if l.memtable.IsFull() {
		if err := l.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
		// Compaction is now handled in flushMemtable()
	}

	return nil
}

// Get retrieves a value by key
func (l *LSM) Get(ctx context.Context, key []byte) (*types.Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, fmt.Errorf("LSM tree is closed")
	}

	// First check memtable
	if entry, err := l.memtable.Get(key); err != nil {
		return nil, fmt.Errorf("failed to get from memtable: %w", err)
	} else if entry != nil {
		if entry.Tombstone {
			return nil, nil // Key was deleted
		}
		return entry, nil
	}

	// Check immutable memtable if it exists
	if l.memtable != nil {
		if entry, err := l.memtable.Get(key); err != nil {
			return nil, fmt.Errorf("failed to get from immutable memtable: %w", err)
		} else if entry != nil {
			if entry.Tombstone {
				return nil, nil // Key was deleted
			}
			return entry, nil
		}
	}

	// Search in SSTables (from newest to oldest)
	levels := l.manifest.GetLevels()
	for _, level := range levels {
		sstables := l.manifest.GetSSTables(level)

		// Search in reverse order (newest first)
		for j := len(sstables) - 1; j >= 0; j-- {
			sstable := sstables[j]
			if entry, err := sstable.Get(key); err != nil {
				return nil, fmt.Errorf("failed to get from SSTable: %w", err)
			} else if entry != nil {
				if entry.Tombstone {
					return nil, nil // Key was deleted
				}
				return entry, nil
			}
		}
	}

	return nil, nil // Key not found
}

// Delete marks a key as deleted
func (l *LSM) Delete(ctx context.Context, key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return fmt.Errorf("LSM tree is closed")
	}

	entry := types.NewDeleteEntry(key)

	// Write to WAL first
	if err := l.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write delete to WAL: %w", err)
	}

	// Write delete marker to memtable
	if err := l.memtable.Delete(key); err != nil {
		return fmt.Errorf("failed to delete from memtable: %w", err)
	}

	// Check if memtable is full
	if l.memtable.IsFull() {
		if err := l.flushMemtable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
		// Compaction is now handled in flushMemtable()
	}

	return nil
}

// Close closes the LSM tree
func (l *LSM) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	// Flush memtable before closing
	if err := l.flushMemtable(); err != nil {
		return fmt.Errorf("failed to flush memtable on close: %w", err)
	}

	// Close all components
	if err := l.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	if err := l.manifest.Close(); err != nil {
		return fmt.Errorf("failed to close manifest: %w", err)
	}

	l.closed = true

	return nil
}

// flushMemtable flushes the current memtable to disk
func (l *LSM) flushMemtable() error {
	if l.memtable.Size() == 0 {
		return nil
	}

	// Snapshot current memtable to flush, and create a new active memtable
	oldMemtable := l.memtable
	l.memtable = memtable.NewMemTable(l.config.MaxMemTableSize)

	// Flush the snapshot to SSTable first
	if err := l.flushMemtableToSSTable(oldMemtable); err != nil {
		return fmt.Errorf("failed to flush immutable memtable: %w", err)
	}

	// Clear WAL only after a successful flush
	if err := l.wal.Clear(); err != nil {
		return fmt.Errorf("failed to clear WAL: %w", err)
	}

	// Trigger immediate compaction after flush
	go l.compactUnlocked()

	return nil
}

// flushImmutableMemtable flushes the immutable memtable to a new SSTable
func (l *LSM) flushMemtableStep2() error {
	if l.memtable == nil {
		return nil
	}

	// Collect all entries from immutable memtable into linked list
	entriesList := list.New()
	iter := l.memtable.Iterator()
	defer iter.Close()

	for iter.Next() {
		entry := iter.Entry()
		if entry != nil {
			entriesList.PushBack(entry)
		}
	}

	if entriesList.Len() == 0 {
		return nil
	}

	// Create new SSTable from linked list
	sstablePath := filepath.Join(l.config.DataDir, fmt.Sprintf("sstable_%d_%d.sst", 0, time.Now().UnixNano()))
	sstable, err := sstable.NewSSTable(sstablePath, entriesList)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	// Add to manifest
	if err := l.manifest.AddSSTable(0, sstable); err != nil {
		sstable.Close()
		return fmt.Errorf("failed to add SSTable to manifest: %w", err)
	}

	// Clear immutable memtable
	if err := l.memtable.Clear(); err != nil {
		return fmt.Errorf("failed to clear immutable memtable: %w", err)
	}

	return nil
}

// recoverFromWAL recovers the memtable from WAL
func (l *LSM) recoverFromWAL() error {
	entries, err := l.wal.Read()
	if err != nil {
		return fmt.Errorf("failed to read from WAL: %w", err)
	}

	// Replay entries to memtable
	for _, entry := range entries {
		if entry.Tombstone {
			if err := l.memtable.Delete(entry.Key); err != nil {
				return fmt.Errorf("failed to replay delete: %w", err)
			}
		} else {
			if err := l.memtable.Put(entry.Key, entry.Value); err != nil {
				return fmt.Errorf("failed to replay put: %w", err)
			}
		}
	}

	return nil
}

// compact performs compaction (with lock)
func (l *LSM) compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.compactUnlocked()
}

// compactUnlocked performs compaction (without lock - for internal use)
func (l *LSM) compactUnlocked() error {
	// Check all levels for compaction
	for level := 0; level < l.config.MaxLevels; level++ {
		if l.shouldCompactLevel(level) {
			if err := l.compactLevel(level, level+1); err != nil {
				return fmt.Errorf("failed to compact level %d: %w", level, err)
			}
			// Only compact one level at a time to avoid overwhelming the system
			return nil
		}
	}

	return nil
}

// shouldCompactLevel determines if a level should be compacted
func (l *LSM) shouldCompactLevel(level int) bool {
	sstables := l.manifest.GetSSTables(level)

	if len(sstables) == 0 {
		return false
	}

	// Level 0: compact when we have too many SSTables
	if level == 0 {
		return len(sstables) >= l.config.CompactionThreshold
	}

	// Level 1+: compact when total size exceeds threshold
	// Calculate total size of SSTables in this level
	totalSize := int64(0)
	for _, sstable := range sstables {
		totalSize += sstable.Size()
	}

	// Calculate size threshold for this level
	// Level 1: baseSize * multiplier^1
	// Level 2: baseSize * multiplier^2
	// etc.
	baseSize := int64(10 * 1024 * 1024) // 10MB base size
	multiplier := int64(l.config.LevelSizeMultiplier)
	levelThreshold := baseSize
	for i := 0; i < level; i++ {
		levelThreshold *= multiplier
	}

	return totalSize >= levelThreshold
}

// compactLevel compacts SSTables from one level to the next
func (l *LSM) compactLevel(fromLevel, toLevel int) error {
	// Get SSTables to compact
	allSSTables := l.manifest.GetSSTables(fromLevel)
	if len(allSSTables) == 0 {
		return nil
	}

	// Find overlapping SSTables in target level
	overlappingSSTables := l.findOverlappingSSTables(allSSTables, toLevel)

	// Combine source and overlapping SSTables for merging
	sstablesToMerge := append(allSSTables, overlappingSSTables...)

	// Merge SSTables using priority queue and linked list
	mergedEntries, err := l.mergeSSTablesWithPriorityQueue(sstablesToMerge)
	if err != nil {
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Create new SSTable from linked list
	sstablePath := filepath.Join(l.config.DataDir, fmt.Sprintf("sstable_%d_%d.sst", toLevel, time.Now().UnixNano()))
	newSSTable, err := l.createSSTableFromLinkedList(sstablePath, mergedEntries)
	if err != nil {
		return fmt.Errorf("failed to create new SSTable: %w", err)
	}

	// Add new SSTable to manifest first to avoid transient read gaps
	if err := l.manifest.AddSSTable(toLevel, newSSTable); err != nil {
		newSSTable.Close()
		return fmt.Errorf("failed to add new SSTable: %w", err)
	}

	// Now remove old SSTables from manifest
	// Remove source level SSTables
	for _, oldSSTable := range allSSTables {
		if err := l.manifest.RemoveSSTable(fromLevel, oldSSTable); err != nil {
			newSSTable.Close()
			return fmt.Errorf("failed to remove old SSTable from level %d: %w", fromLevel, err)
		}
		oldSSTable.Close()
	}

	// Remove overlapping SSTables from target level
	for _, oldSSTable := range overlappingSSTables {
		if err := l.manifest.RemoveSSTable(toLevel, oldSSTable); err != nil {
			newSSTable.Close()
			return fmt.Errorf("failed to remove overlapping SSTable from level %d: %w", toLevel, err)
		}
		oldSSTable.Close()
	}

	return nil
}

// findOverlappingSSTables finds SSTables in target level that overlap with source SSTables
func (l *LSM) findOverlappingSSTables(sourceSSTables []sstable.SSTable, targetLevel int) []sstable.SSTable {
	targetSSTables := l.manifest.GetSSTables(targetLevel)
	if len(targetSSTables) == 0 {
		return nil
	}

	// Find the overall min and max keys of all source SSTables
	sourceMinKey, sourceMaxKey := l.getOverallKeyRange(sourceSSTables)
	if sourceMinKey == nil || sourceMaxKey == nil {
		return nil
	}

	var overlapping []sstable.SSTable

	// Find target SSTables that overlap with the overall source key range
	for _, targetSSTable := range targetSSTables {
		targetMinKey := targetSSTable.MinKey()
		targetMaxKey := targetSSTable.MaxKey()

		// Check if key ranges overlap
		if l.keyRangesOverlap(sourceMinKey, sourceMaxKey, targetMinKey, targetMaxKey) {
			overlapping = append(overlapping, targetSSTable)
		}
	}

	return overlapping
}

// getOverallKeyRange finds the min and max keys across all SSTables
func (l *LSM) getOverallKeyRange(sstables []sstable.SSTable) ([]byte, []byte) {
	if len(sstables) == 0 {
		return nil, nil
	}

	var minKey, maxKey []byte

	for _, sstable := range sstables {
		sstableMinKey := sstable.MinKey()
		sstableMaxKey := sstable.MaxKey()

		if sstableMinKey == nil || sstableMaxKey == nil {
			continue
		}

		// Initialize min/max with first SSTable
		if minKey == nil {
			minKey = sstableMinKey
			maxKey = sstableMaxKey
			continue
		}

		// Update min key
		if bytes.Compare(sstableMinKey, minKey) < 0 {
			minKey = sstableMinKey
		}

		// Update max key
		if bytes.Compare(sstableMaxKey, maxKey) > 0 {
			maxKey = sstableMaxKey
		}
	}

	return minKey, maxKey
}

// keyRangesOverlap checks if two key ranges overlap
func (l *LSM) keyRangesOverlap(min1, max1, min2, max2 []byte) bool {
	// Two ranges overlap if: min1 <= max2 AND min2 <= max1
	// Handle nil keys (empty ranges)
	if min1 == nil || max1 == nil || min2 == nil || max2 == nil {
		return false
	}

	// Check overlap condition
	return bytes.Compare(min1, max2) <= 0 && bytes.Compare(min2, max1) <= 0
}

// ForceCompaction triggers compaction for all levels that need it
func (l *LSM) ForceCompaction() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check all levels for compaction
	for level := 0; level < l.config.MaxLevels; level++ {
		if l.shouldCompactLevel(level) {
			if err := l.compactLevel(level, level+1); err != nil {
				return fmt.Errorf("failed to compact level %d: %w", level, err)
			}
		}
	}

	return nil
}

// GetCompactionStatus returns information about compaction status
func (l *LSM) GetCompactionStatus() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	status := make(map[string]interface{})

	for level := 0; level < l.config.MaxLevels; level++ {
		sstables := l.manifest.GetSSTables(level)
		if len(sstables) == 0 {
			continue
		}

		// Calculate total size
		totalSize := int64(0)
		for _, sstable := range sstables {
			totalSize += sstable.Size()
		}

		// Calculate threshold
		var threshold int64
		if level == 0 {
			threshold = int64(l.config.CompactionThreshold)
		} else {
			baseSize := int64(10 * 1024 * 1024) // 10MB
			multiplier := int64(l.config.LevelSizeMultiplier)
			threshold = baseSize
			for i := 0; i < level; i++ {
				threshold *= multiplier
			}
		}

		levelStatus := map[string]interface{}{
			"sstable_count":    len(sstables),
			"total_size":       totalSize,
			"threshold":        threshold,
			"needs_compaction": l.shouldCompactLevel(level),
		}

		status[fmt.Sprintf("level_%d", level)] = levelStatus
	}

	return status
}

// mergeSSTablesWithPriorityQueue merges multiple SSTables using a priority queue and returns a linked list
func (l *LSM) mergeSSTablesWithPriorityQueue(sstables []sstable.SSTable) (*list.List, error) {
	if len(sstables) == 0 {
		return nil, nil
	}
	lessFn := func(a, b *sstable.IteratorItem) bool {
		// First compare by key
		keyCompare := bytes.Compare(a.Entry.Key, b.Entry.Key)
		if keyCompare != 0 {
			return keyCompare < 0
		}

		// If keys are equal, compare by timestamp (newer first)
		if a.Entry.Timestamp != b.Entry.Timestamp {
			return a.Entry.Timestamp > b.Entry.Timestamp
		}

		// If timestamps are equal, use index for stable sorting
		return a.Index < b.Index
	}
	// Create priority queue
	pq := priorityqueue.NewPriorityQueue[*sstable.IteratorItem](lessFn)
	heap.Init(pq)

	// Initialize iterators for all SSTables
	iterators := make([]iterator.Iterator[*types.Entry], len(sstables))
	for i, table := range sstables {
		iterators[i] = table.Iterator()
	}

	// Add first entry from each SSTable to the priority queue
	for i, iter := range iterators {
		if iter.Next() {
			entry := iter.Entry()
			if entry != nil {
				item := &sstable.IteratorItem{
					Entry:    entry,
					Iterator: iter,
					Index:    i,
				}
				heap.Push(pq, item)
			}
		}
	}

	// Create linked list for merged entries
	mergedList := list.New()
	var lastKey []byte
	var lastEntry *types.Entry

	// Process entries from priority queue
	for pq.Len() > 0 {
		// Get the smallest entry
		item := heap.Pop(pq).(*sstable.IteratorItem)
		currentEntry := item.Entry

		// Handle duplicate keys (keep the latest version)
		if lastKey != nil && bytes.Equal(currentEntry.Key, lastKey) {
			// Same key, keep the newer entry
			if currentEntry.Timestamp > lastEntry.Timestamp {
				// Replace the last entry with current entry
				mergedList.Back().Value = currentEntry
				lastEntry = currentEntry
			}
		} else {
			// New key, add to linked list
			mergedList.PushBack(currentEntry)
			lastKey = currentEntry.Key
			lastEntry = currentEntry
		}

		// Get next entry from the same SSTable
		if item.Iterator.Next() {
			nextEntry := item.Iterator.Entry()
			if nextEntry != nil {
				item.Entry = nextEntry
				heap.Push(pq, item)
			}
		}
	}

	// Close all iterators
	for _, iter := range iterators {
		iter.Close()
	}

	// Return linked list directly
	return mergedList, nil
}

// createSSTableFromLinkedList creates a new SSTable from a linked list of entries
func (l *LSM) createSSTableFromLinkedList(path string, mergedList *list.List) (sstable.SSTable, error) {
	if mergedList == nil || mergedList.Len() == 0 {
		return nil, fmt.Errorf("no entries to create SSTable")
	}

	// Create SSTable directly from linked list
	sstable, err := sstable.NewSSTable(path, mergedList)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable: %w", err)
	}

	return sstable, nil
}

func (l *LSM) flushMemtableToSSTable(m memtable.MemTable) error {
	if m == nil || m.Size() == 0 {
		return nil
	}

	// Collect all entries from immutable memtable into linked list
	entriesList := list.New()
	iter := m.Iterator()
	defer iter.Close()

	for iter.Next() {
		entry := iter.Entry()
		if entry != nil {
			entriesList.PushBack(entry)
		}
	}

	if entriesList.Len() == 0 {
		return nil
	}

	// Create new SSTable from linked list
	sstablePath := filepath.Join(l.config.DataDir, fmt.Sprintf("sstable_%d_%d.sst", 0, time.Now().UnixNano()))
	sstableObj, err := sstable.NewSSTable(sstablePath, entriesList)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	// Add to manifest
	if err := l.manifest.AddSSTable(0, sstableObj); err != nil {
		sstableObj.Close()
		return fmt.Errorf("failed to add SSTable to manifest: %w", err)
	}

	// Clear immutable memtable snapshot
	if err := m.Clear(); err != nil {
		return fmt.Errorf("failed to clear immutable memtable: %w", err)
	}

	return nil
}
