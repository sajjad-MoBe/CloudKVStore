package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
)

// LogEntry represents a single entry in the WAL
type LogEntry struct {
	Timestamp time.Time   `json:"timestamp"`
	Operation string      `json:"operation"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Partition int         `json:"partition"`
}

// WAL represents the Write-Ahead Log
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	path        string
	maxSize     int64
	currentSize int64
	logger      *shared.Logger
	metrics     *shared.MetricsCollector
}

// NewWAL creates a new WAL instance
func NewWAL(path string, maxSize int64) (*WAL, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	// Open or create WAL file
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	return &WAL{
		file:        file,
		path:        path,
		maxSize:     maxSize,
		currentSize: info.Size(),
		logger:      shared.DefaultLogger,
		metrics:     shared.DefaultCollector,
	}, nil
}

// Write appends a new entry to the WAL
func (w *WAL) Write(operation, key string, value interface{}, partition int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Create log entry
	entry := LogEntry{
		Timestamp: time.Now(),
		Operation: operation,
		Key:       key,
		Value:     value,
		Partition: partition,
	}

	// Marshal entry to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %v", err)
	}

	// Check if we need to rotate the log
	if w.currentSize+int64(len(data)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("failed to rotate WAL: %v", err)
		}
	}

	// Write entry to file
	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Update current size
	w.currentSize += int64(len(data)) + 1

	// Record metrics
	w.metrics.RecordMetric(shared.MetricWALWriteCount, shared.Counter, 1, nil)
	w.metrics.RecordMetric(shared.MetricWALSize, shared.Gauge, float64(w.currentSize), nil)

	return nil
}

// Read reads all entries from the WAL
func (w *WAL) Read() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset file position
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to start of WAL: %v", err)
	}

	var entries []LogEntry
	decoder := json.NewDecoder(w.file)

	for decoder.More() {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			return nil, fmt.Errorf("failed to decode log entry: %v", err)
		}
		entries = append(entries, entry)
	}

	// Record metrics
	w.metrics.RecordMetric(shared.MetricWALReadCount, shared.Counter, 1, nil)

	return entries, nil
}

// rotate rotates the WAL file
func (w *WAL) rotate() error {
	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %v", err)
	}

	// Create backup filename with timestamp
	backupPath := fmt.Sprintf("%s.%d", w.path, time.Now().UnixNano())

	// Rename current file to backup
	if err := os.Rename(w.path, backupPath); err != nil {
		return fmt.Errorf("failed to rename WAL file: %v", err)
	}

	// Create new WAL file
	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %v", err)
	}

	w.file = file
	w.currentSize = 0

	// Record metrics
	w.metrics.RecordMetric(shared.MetricWALRotationCount, shared.Counter, 1, nil)

	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %v", err)
	}

	return nil
}
