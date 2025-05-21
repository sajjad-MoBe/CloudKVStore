package storage

import (
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/wal"
	"sync"
	"sync/atomic"
	"time"
)

// StorageEngine defines the interface for the key-value store
type StorageEngine interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	CreateSnapshot() (string, error)
	RestoreSnapshot(path string) error
	Batch(operations []BatchOperation) error
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	GetMetrics() *StorageMetrics
	Cleanup() error
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Type  string // "SET" or "DELETE"
	Key   string
	Value []byte
	TTL   time.Duration
}

// StorageMetrics tracks storage engine metrics
type StorageMetrics struct {
	TotalKeys    int64
	TotalSize    int64
	ReadCount    int64
	WriteCount   int64
	DeleteCount  int64
	ErrorCount   int64
	LastCleanup  time.Time
	CleanupCount int64
}

// MemTable implements the StorageEngine interface with in-memory storage
type MemTable struct {
	data        map[string]*entry
	mutex       sync.RWMutex
	walWriter   wal.WALWriter
	snapshotter Snapshotter
	metrics     *StorageMetrics
	maxSize     int64
	cleanupCh   chan struct{}
	stopCh      chan struct{}
}

// entry represents a key-value pair with metadata
type entry struct {
	value     []byte
	expiresAt time.Time
}

// NewMemTable creates a new MemTable instance
func NewMemTable(walWriter wal.WALWriter, snapshotter Snapshotter, maxSize int64) *MemTable {
	m := &MemTable{
		data:        make(map[string]*entry),
		walWriter:   walWriter,
		snapshotter: snapshotter,
		metrics:     &StorageMetrics{},
		maxSize:     maxSize,
		cleanupCh:   make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}

	// Start cleanup goroutine
	go m.cleanupLoop()
	return m
}

// Get retrieves a value for the given key
func (m *MemTable) Get(key string) ([]byte, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	atomic.AddInt64(&m.metrics.ReadCount, 1)

	entry, exists := m.data[key]
	if !exists {
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return nil, &ErrKeyNotFound{Key: key}
	}

	// Check if entry has expired
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		delete(m.data, key)
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return nil, &ErrKeyNotFound{Key: key}
	}

	return entry.value, nil
}

// Set stores a value for the given key
func (m *MemTable) Set(key string, value []byte) error {
	return m.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a value for the given key with TTL
func (m *MemTable) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	newSize := atomic.AddInt64(&m.metrics.TotalSize, int64(len(value)))
	if m.maxSize > 0 && newSize > m.maxSize {
		atomic.AddInt64(&m.metrics.TotalSize, -int64(len(value)))
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return &ErrStorageFull{CurrentSize: newSize, MaxSize: m.maxSize}
	}

	walEntry := &wal.WALEntry{
		Operation: "SET",
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	if err := m.walWriter.Append(walEntry); err != nil {
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return err
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	// explicitly reference internal type "entry"
	m.data[key] = &entry{
		value:     value,
		expiresAt: expiresAt,
	}

	atomic.AddInt64(&m.metrics.WriteCount, 1)
	atomic.AddInt64(&m.metrics.TotalKeys, 1)
	return nil
}

// Delete removes a key-value pair
func (m *MemTable) Delete(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	walEntry := &wal.WALEntry{
		Operation: "DELETE",
		Key:       key,
		Timestamp: time.Now().UnixNano(),
	}

	if err := m.walWriter.Append(walEntry); err != nil {
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return err
	}

	if oldEntry, exists := m.data[key]; exists {
		atomic.AddInt64(&m.metrics.TotalSize, -int64(len(oldEntry.value)))
	}
	delete(m.data, key)
	atomic.AddInt64(&m.metrics.DeleteCount, 1)
	atomic.AddInt64(&m.metrics.TotalKeys, -1)
	return nil
}

// Batch executes multiple operations atomically
func (m *MemTable) Batch(operations []BatchOperation) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Create WAL entries for all operations
	for _, op := range operations {
		walEntry := &wal.WALEntry{
			Operation: op.Type,
			Key:       op.Key,
			Value:     op.Value,
			Timestamp: time.Now().UnixNano(),
		}
		if err := m.walWriter.Append(walEntry); err != nil {
			atomic.AddInt64(&m.metrics.ErrorCount, 1)
			return err
		}
	}

	// Apply all operations
	for _, op := range operations {
		switch op.Type {
		case "SET":
			var expiresAt time.Time
			if op.TTL > 0 {
				expiresAt = time.Now().Add(op.TTL)
			}
			if oldEntry, exists := m.data[op.Key]; exists {
				atomic.AddInt64(&m.metrics.TotalSize, -int64(len(oldEntry.value)))
			}
			m.data[op.Key] = &entry{
				value:     op.Value,
				expiresAt: expiresAt,
			}
			atomic.AddInt64(&m.metrics.TotalSize, int64(len(op.Value)))
			atomic.AddInt64(&m.metrics.WriteCount, 1)
			atomic.AddInt64(&m.metrics.TotalKeys, 1)

		case "DELETE":
			if oldEntry, exists := m.data[op.Key]; exists {
				atomic.AddInt64(&m.metrics.TotalSize, -int64(len(oldEntry.value)))
				delete(m.data, op.Key)
				atomic.AddInt64(&m.metrics.DeleteCount, 1)
				atomic.AddInt64(&m.metrics.TotalKeys, -1)
			}
		}
	}

	return nil
}

// CreateSnapshot creates a snapshot of the current state
func (m *MemTable) CreateSnapshot() (string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create a copy of the current data
	dataCopy := make(map[string][]byte)
	for k, v := range m.data {
		// Only include non-expired entries
		if v.expiresAt.IsZero() || time.Now().Before(v.expiresAt) {
			dataCopy[k] = v.value
		}
	}

	// Create snapshot using the snapshotter
	return m.snapshotter.Create(dataCopy)
}

// RestoreSnapshot restores the state from a snapshot
func (m *MemTable) RestoreSnapshot(path string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Load snapshot data
	data, err := m.snapshotter.Restore(path)
	if err != nil {
		atomic.AddInt64(&m.metrics.ErrorCount, 1)
		return err
	}

	// Replace current data with snapshot data
	m.data = make(map[string]*entry)
	var totalSize int64
	for k, v := range data {
		m.data[k] = &entry{value: v}
		totalSize += int64(len(v))
	}

	// Update metrics
	atomic.StoreInt64(&m.metrics.TotalKeys, int64(len(data)))
	atomic.StoreInt64(&m.metrics.TotalSize, totalSize)
	return nil
}

// GetMetrics returns the current storage metrics
func (m *MemTable) GetMetrics() *StorageMetrics {
	return &StorageMetrics{
		TotalKeys:    atomic.LoadInt64(&m.metrics.TotalKeys),
		TotalSize:    atomic.LoadInt64(&m.metrics.TotalSize),
		ReadCount:    atomic.LoadInt64(&m.metrics.ReadCount),
		WriteCount:   atomic.LoadInt64(&m.metrics.WriteCount),
		DeleteCount:  atomic.LoadInt64(&m.metrics.DeleteCount),
		ErrorCount:   atomic.LoadInt64(&m.metrics.ErrorCount),
		LastCleanup:  m.metrics.LastCleanup,
		CleanupCount: atomic.LoadInt64(&m.metrics.CleanupCount),
	}
}

// Cleanup removes expired entries
func (m *MemTable) Cleanup() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	now := time.Now()
	var cleaned int64
	for k, v := range m.data {
		if !v.expiresAt.IsZero() && now.After(v.expiresAt) {
			atomic.AddInt64(&m.metrics.TotalSize, -int64(len(v.value)))
			delete(m.data, k)
			atomic.AddInt64(&m.metrics.TotalKeys, -1)
			cleaned++
		}
	}

	if cleaned > 0 {
		atomic.AddInt64(&m.metrics.CleanupCount, 1)
		m.metrics.LastCleanup = now
	}
	return nil
}

// cleanupLoop periodically runs cleanup
func (m *MemTable) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := m.Cleanup()
			if err != nil {
				return
			}
		case <-m.cleanupCh:
			err := m.Cleanup()
			if err != nil {
				return
			}
		case <-m.stopCh:
			return
		}
	}
}

// GetAll returns a snapshot of all non-expired key→value pairs.
func (m *MemTable) GetAll() map[string][]byte {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string][]byte, len(m.data))
	now := time.Now()
	for k, e := range m.data {
		// skip expired entries
		if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
			result[k] = e.value
		}
	}
	return result
}

// ErrKeyNotFound is returned when a key is not found in the store
type ErrKeyNotFound struct {
	Key string
}

func (e *ErrKeyNotFound) Error() string {
	return "key not found: " + e.Key
}

// ErrStorageFull is returned when the storage size limit is reached
type ErrStorageFull struct {
	CurrentSize int64
	MaxSize     int64
}

func (e *ErrStorageFull) Error() string {
	return "storage full: current size exceeds maximum size"
}
