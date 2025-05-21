package storage

import (
	"fmt"
	"os"
	_ "path/filepath"
	"testing"
)

// mockWALWriter implements WALWriter for testing
type mockWALWriter struct {
	entries []*WALEntry
}

func (m *mockWALWriter) Append(entry *WALEntry) error {
	m.entries = append(m.entries, entry)
	return nil
}

func (m *mockWALWriter) Sync() error {
	return nil
}

func (m *mockWALWriter) Close() error {
	return nil
}

func setupTest(t *testing.T) (*MemTable, string, func()) {
	// Create temporary directory for snapshots
	tempDir, err := os.MkdirTemp("", "kvstore_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create mock WAL writer
	walWriter := &mockWALWriter{}

	// Create snapshotter
	snapshotter, err := NewFileSnapshotter(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create snapshotter: %v", err)
	}

	// Create MemTable
	table := NewMemTable(walWriter, snapshotter)

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return table, tempDir, cleanup
}

func TestMemTableSetGet(t *testing.T) {
	table, _, cleanup := setupTest(t)
	defer cleanup()

	// Test Set and Get
	key := "test-key"
	value := []byte("test-value")

	err := table.Set(key, value)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	got, err := table.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Get returned wrong value: got %v, want %v", string(got), string(value))
	}
}

func TestMemTableDelete(t *testing.T) {
	table, _, cleanup := setupTest(t)
	defer cleanup()

	// Test Delete
	key := "test-key"
	value := []byte("test-value")

	// Set a value first
	err := table.Set(key, value)
	if err != nil {
		t.Errorf("Set failed: %v", err)
	}

	// Delete the value
	err = table.Delete(key)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// Try to get the deleted value
	_, err = table.Get(key)
	if err == nil {
		t.Error("Expected error for deleted key, got nil")
	}

	if _, ok := err.(*ErrKeyNotFound); !ok {
		t.Errorf("Expected ErrKeyNotFound, got %T", err)
	}
}

func TestMemTableSnapshot(t *testing.T) {
	table, tempDir, cleanup := setupTest(t)
	defer cleanup()

	// Set some test data
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	for k, v := range testData {
		if err := table.Set(k, v); err != nil {
			t.Errorf("Set failed for %s: %v", k, err)
		}
	}

	// Create snapshot
	snapshotPath, err := table.CreateSnapshot()
	if err != nil {
		t.Errorf("CreateSnapshot failed: %v", err)
	}

	// Verify snapshot file exists
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Errorf("Snapshot file not created: %v", err)
	}

	// Create new table and restore snapshot
	newTable := NewMemTable(&mockWALWriter{}, table.snapshotter)
	if err := newTable.RestoreSnapshot(snapshotPath); err != nil {
		t.Errorf("RestoreSnapshot failed: %v", err)
	}

	// Verify restored data
	for k, v := range testData {
		got, err := newTable.Get(k)
		if err != nil {
			t.Errorf("Get failed for %s: %v", k, err)
		}
		if string(got) != string(v) {
			t.Errorf("Restored value mismatch for %s: got %v, want %v", k, string(got), string(v))
		}
	}
}

func TestMemTableConcurrent(t *testing.T) {
	table, _, cleanup := setupTest(t)
	defer cleanup()

	// Test concurrent access
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))

			// Set
			if err := table.Set(key, value); err != nil {
				t.Errorf("Concurrent Set failed: %v", err)
			}

			// Get
			if got, err := table.Get(key); err != nil {
				t.Errorf("Concurrent Get failed: %v", err)
			} else if string(got) != string(value) {
				t.Errorf("Concurrent Get returned wrong value: got %v, want %v", string(got), string(value))
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
