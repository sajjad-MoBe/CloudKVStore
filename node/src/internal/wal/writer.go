package wal

import (
	"encoding/gob"
	"os"
	"sync"
)

// WALEntry represents a single write-ahead log entry
type WALEntry struct {
	Operation string // "SET" or "DELETE"
	Key       string
	Value     []byte
	Timestamp int64
}

// WALWriter defines the interface for WAL operations
type WALWriter interface {
	Append(entry *WALEntry) error
	Sync() error
	Close() error
}

// FileWAL implements WALWriter using a file-based approach
type FileWAL struct {
	file     *os.File
	encoder  *gob.Encoder
	syncMode bool
	mu       sync.Mutex
}

// NewFileWAL creates a new FileWAL instance
func NewFileWAL(path string, syncMode bool) (*FileWAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &FileWAL{
		file:     file,
		encoder:  gob.NewEncoder(file),
		syncMode: syncMode,
	}, nil
}

// Append writes a new entry to the WAL
func (w *FileWAL) Append(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.encoder.Encode(entry); err != nil {
		return err
	}

	if w.syncMode {
		return w.Sync()
	}
	return nil
}

// Sync ensures all buffered data is written to disk
func (w *FileWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Implement WAL fsync policy
	return w.file.Sync()
}

// Close closes the WAL file
func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}
