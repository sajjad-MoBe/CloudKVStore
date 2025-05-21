package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupWALTest(t *testing.T) (*WALManager, func()) {
	// Create temporary directory
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create WAL manager
	config := WALConfig{
		MaxFileSize:    1024, // 1KB
		MaxFiles:       3,
		RotationPeriod: time.Minute,
		CompressFiles:  false,
	}

	manager, err := NewWALManager(dir, config)
	if err != nil {
		err := os.RemoveAll(dir)
		if err != nil {
			return nil, nil
		}
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		err := manager.Close()
		if err != nil {
			return
		}
		err = os.RemoveAll(dir)
		if err != nil {
			return
		}
	}

	return manager, cleanup
}

func TestWALAppend(t *testing.T) {
	manager, cleanup := setupWALTest(t)
	defer cleanup()

	// Create test entry
	entry := &WALEntry{
		Operation: "SET",
		Key:       "test-key",
		Value:     []byte("test-value"),
		Timestamp: time.Now().UnixNano(),
	}

	// Append entry
	if err := manager.Append(entry); err != nil {
		t.Errorf("Failed to append entry: %v", err)
	}

	// Verify metrics
	metrics := manager.GetMetrics()
	if metrics.TotalEntries != 1 {
		t.Errorf("Expected 1 entry, got %d", metrics.TotalEntries)
	}
}

func TestWALRotation(t *testing.T) {
	manager, cleanup := setupWALTest(t)
	defer cleanup()

	// Create entries that will trigger rotation
	largeValue := make([]byte, 1024) // 1KB
	for i := 0; i < 5; i++ {
		entry := &WALEntry{
			Operation: "SET",
			Key:       "test-key",
			Value:     largeValue,
			Timestamp: time.Now().UnixNano(),
		}
		if err := manager.Append(entry); err != nil {
			t.Errorf("Failed to append entry: %v", err)
		}
	}

	// Verify rotation
	metrics := manager.GetMetrics()
	if metrics.RotationCount < 1 {
		t.Error("Expected rotation to occur")
	}
}

func TestWALRecovery(t *testing.T) {
	manager, cleanup := setupWALTest(t)
	defer cleanup()

	// Create test entries
	entries := []*WALEntry{
		{
			Operation: "SET",
			Key:       "key1",
			Value:     []byte("value1"),
			Timestamp: time.Now().UnixNano(),
		},
		{
			Operation: "SET",
			Key:       "key2",
			Value:     []byte("value2"),
			Timestamp: time.Now().UnixNano(),
		},
		{
			Operation: "DELETE",
			Key:       "key1",
			Timestamp: time.Now().UnixNano(),
		},
	}

	// Append entries
	for _, entry := range entries {
		if err := manager.Append(entry); err != nil {
			t.Errorf("Failed to append entry: %v", err)
		}
	}

	// Recover entries
	recovered := make([]*WALEntry, 0)
	handler := func(entry *WALEntry) error {
		recovered = append(recovered, entry)
		return nil
	}

	if err := manager.Recover(handler); err != nil {
		t.Errorf("Failed to recover entries: %v", err)
	}

	// Verify recovered entries
	if len(recovered) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(recovered))
	}

	for i, entry := range entries {
		if recovered[i].Operation != entry.Operation {
			t.Errorf("Expected operation %s, got %s", entry.Operation, recovered[i].Operation)
		}
		if recovered[i].Key != entry.Key {
			t.Errorf("Expected key %s, got %s", entry.Key, recovered[i].Key)
		}
	}
}

func TestWALCleanup(t *testing.T) {
	manager, cleanup := setupWALTest(t)
	defer cleanup()

	// Create entries that will trigger multiple rotations
	largeValue := make([]byte, 1024) // 1KB
	for i := 0; i < 10; i++ {
		entry := &WALEntry{
			Operation: "SET",
			Key:       "test-key",
			Value:     largeValue,
			Timestamp: time.Now().UnixNano(),
		}
		if err := manager.Append(entry); err != nil {
			t.Errorf("Failed to append entry: %v", err)
		}
	}

	// List WAL files
	files, err := filepath.Glob(filepath.Join(manager.dir, "wal-*.log"))
	if err != nil {
		t.Errorf("Failed to list WAL files: %v", err)
	}

	// Verify we don't have more than MaxFiles
	if len(files) > manager.config.MaxFiles {
		t.Errorf("Expected at most %d files, got %d", manager.config.MaxFiles, len(files))
	}
}

func TestWALMetrics(t *testing.T) {
	manager, cleanup := setupWALTest(t)
	defer cleanup()

	// Create test entries
	entries := []*WALEntry{
		{
			Operation: "SET",
			Key:       "key1",
			Value:     []byte("value1"),
			Timestamp: time.Now().UnixNano(),
		},
		{
			Operation: "SET",
			Key:       "key2",
			Value:     []byte("value2"),
			Timestamp: time.Now().UnixNano(),
		},
	}

	// Append entries
	for _, entry := range entries {
		if err := manager.Append(entry); err != nil {
			t.Errorf("Failed to append entry: %v", err)
		}
	}

	// Verify metrics
	metrics := manager.GetMetrics()
	if metrics.TotalEntries != int64(len(entries)) {
		t.Errorf("Expected %d entries, got %d", len(entries), metrics.TotalEntries)
	}
	if metrics.TotalSize == 0 {
		t.Error("Expected non-zero total size")
	}
	if metrics.ErrorCount != 0 {
		t.Errorf("Expected 0 errors, got %d", metrics.ErrorCount)
	}
}
