package storage

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Snapshotter defines the interface for snapshot operations
type Snapshotter interface {
	Create(data map[string][]byte) (string, error)
	Restore(path string) (map[string][]byte, error)
}

// FileSnapshotter implements Snapshotter using file-based storage
type FileSnapshotter struct {
	snapshotDir string
}

// NewFileSnapshotter creates a new FileSnapshotter instance
func NewFileSnapshotter(snapshotDir string) (*FileSnapshotter, error) {
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	return &FileSnapshotter{snapshotDir: snapshotDir}, nil
}

// Create saves the current state to a snapshot file
func (s *FileSnapshotter) Create(data map[string][]byte) (string, error) {
	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf("snapshot_%d.gob", timestamp)
	path := filepath.Join(s.snapshotDir, filename)

	file, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		err := os.Remove(path)
		if err != nil {
			return "", err
		} // Clean up on failure
		return "", fmt.Errorf("failed to encode snapshot data: %w", err)
	}

	return path, nil
}

// Restore loads a snapshot from a file
func (s *FileSnapshotter) Restore(path string) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	var data map[string][]byte
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot data: %w", err)
	}

	return data, nil
}
