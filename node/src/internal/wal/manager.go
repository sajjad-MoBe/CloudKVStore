package wal

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// WALConfig contains configuration for WAL management
type WALConfig struct {
	MaxFileSize    int64         // Maximum size of each WAL file in bytes
	MaxFiles       int           // Maximum number of WAL files to retain
	RotationPeriod time.Duration // Interval for periodic rotation
	CompressFiles  bool          // Whether to compress old WAL files
}

// WALMetrics tracks operational metrics for the WAL
type WALMetrics struct {
	TotalEntries     int64     // Total number of entries written
	TotalSize        int64     // Total size of all entries in bytes
	CurrentFileSize  int64     // Size of the current WAL file
	RotationCount    int64     // Number of rotations performed
	LastRotationTime time.Time // Timestamp of last rotation
	ErrorCount       int64     // Number of errors encountered
}

// WALManager manages WAL files and operations
type WALManager struct {
	config     WALConfig
	dir        string
	current    *os.File
	encoder    *gob.Encoder
	metrics    WALMetrics
	mutex      sync.RWMutex
	stopCh     chan struct{}
	rotationCh chan struct{}
	closeOnce  sync.Once
}

// NewWALManager creates a new WAL manager
func NewWALManager(dir string, config WALConfig) (*WALManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	m := &WALManager{
		config:     config,
		dir:        dir,
		stopCh:     make(chan struct{}),
		rotationCh: make(chan struct{}, 1),
	}

	if err := m.rotate(); err != nil {
		return nil, fmt.Errorf("failed to create initial WAL file: %v", err)
	}

	go m.rotationLoop()
	go m.processRotations()

	return m, nil
}

// Append writes an entry to the WAL
func (m *WALManager) Append(entry *WALEntry) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.metrics.CurrentFileSize >= m.config.MaxFileSize {
		if err := m.rotate(); err != nil {
			m.metrics.ErrorCount++
			return fmt.Errorf("failed to rotate WAL: %v", err)
		}
	}

	if err := m.encoder.Encode(entry); err != nil {
		m.metrics.ErrorCount++
		return fmt.Errorf("failed to encode WAL entry: %v", err)
	}

	entrySize := int64(len(entry.Key) + len(entry.Value))
	m.metrics.TotalEntries++
	m.metrics.TotalSize += entrySize
	m.metrics.CurrentFileSize += entrySize

	return nil
}

// Rotate creates a new WAL file
func (m *WALManager) Rotate() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.rotate()
}

func (m *WALManager) rotate() error {
	if m.current != nil {
		if err := m.current.Close(); err != nil {
			return fmt.Errorf("failed to close current WAL file: %v", err)
		}
	}

	timestamp := time.Now().Format("20060102150405")
	filename := filepath.Join(m.dir, fmt.Sprintf("wal-%s.log", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %v", err)
	}

	m.current = file
	m.encoder = gob.NewEncoder(file)
	m.metrics.CurrentFileSize = 0
	m.metrics.RotationCount++
	m.metrics.LastRotationTime = time.Now()

	if err := m.cleanupOldFiles(); err != nil {
		return fmt.Errorf("failed to cleanup old WAL files: %v", err)
	}

	return nil
}

func (m *WALManager) cleanupOldFiles() error {
	files, err := filepath.Glob(filepath.Join(m.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %v", err)
	}

	sort.Strings(files)

	if len(files) > m.config.MaxFiles {
		for _, file := range files[:len(files)-m.config.MaxFiles] {
			if err := os.Remove(file); err != nil {
				return fmt.Errorf("failed to remove old WAL file: %v", err)
			}
		}
	}

	return nil
}

// Recover replays WAL entries to restore state
func (m *WALManager) Recover(handler func(*WALEntry) error) error {
	files, err := filepath.Glob(filepath.Join(m.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %v", err)
	}

	sort.Strings(files)

	for _, file := range files {
		if err := m.replayFile(file, handler); err != nil {
			return fmt.Errorf("failed to replay WAL file %s: %v", file, err)
		}
	}

	return nil
}

func (m *WALManager) replayFile(path string, handler func(*WALEntry) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var entry WALEntry
		if err := decoder.Decode(&entry); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to decode WAL entry: %v", err)
		}

		if err := handler(&entry); err != nil {
			return fmt.Errorf("failed to handle WAL entry: %v", err)
		}
	}

	return nil
}

// GetMetrics returns the current WAL metrics
func (m *WALManager) GetMetrics() *WALMetrics {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return &WALMetrics{
		TotalEntries:     m.metrics.TotalEntries,
		TotalSize:        m.metrics.TotalSize,
		CurrentFileSize:  m.metrics.CurrentFileSize,
		RotationCount:    m.metrics.RotationCount,
		LastRotationTime: m.metrics.LastRotationTime,
		ErrorCount:       m.metrics.ErrorCount,
	}
}

// Close closes the WAL manager
func (m *WALManager) Close() error {
	m.closeOnce.Do(func() {
		close(m.stopCh)
		if m.current != nil {
			m.current.Close()
		}
	})
	return nil
}

func (m *WALManager) rotationLoop() {
	ticker := time.NewTicker(m.config.RotationPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case m.rotationCh <- struct{}{}:
			default: // Skip if rotation already pending
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *WALManager) processRotations() {
	for {
		select {
		case <-m.rotationCh:
			if err := m.Rotate(); err != nil {
				m.mutex.Lock()
				m.metrics.ErrorCount++
				m.mutex.Unlock()
			}
		case <-m.stopCh:
			return
		}
	}
}
