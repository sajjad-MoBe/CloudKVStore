package storage

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupCompactionTest(t *testing.T) (string, func()) {
	// Create temporary directory for snapshots
	tempDir, err := os.MkdirTemp("", "compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

func createTestSnapshot(t *testing.T, dir string, data map[string][]byte) string {
	timestamp := time.Now().UnixNano()
	path := filepath.Join(dir, fmt.Sprintf("snapshot_%d.gob", timestamp))

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create snapshot file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		t.Fatalf("Failed to encode snapshot data: %v", err)
	}

	return path
}

func TestCompactionManager(t *testing.T) {
	tempDir, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create compaction manager with short interval
	manager, err := NewCompactionManager(tempDir, 100*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Create test snapshots
	testData := []map[string][]byte{
		{"key1": []byte("value1"), "key2": []byte("value2")},
		{"key1": []byte("value1_updated"), "key3": []byte("value3")},
		{"key2": []byte("value2_updated"), "key4": []byte("value4")},
		{"key1": []byte("value1_final"), "key5": []byte("value5")},
	}

	for _, data := range testData {
		createTestSnapshot(t, tempDir, data)
	}

	// Start compaction manager
	manager.Start()
	defer manager.Stop()

	// Wait for compaction to occur
	time.Sleep(200 * time.Millisecond)

	// List snapshot files
	files, err := filepath.Glob(filepath.Join(tempDir, "snapshot_*.gob"))
	if err != nil {
		t.Fatalf("Failed to list snapshot files: %v", err)
	}

	// Verify we have the correct number of files
	if len(files) > 3 {
		t.Errorf("Too many snapshot files: got %d, want <= 3", len(files))
	}

	// Verify merged data
	var mergedData map[string][]byte
	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("Failed to open merged snapshot: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&mergedData); err != nil {
		t.Fatalf("Failed to decode merged snapshot: %v", err)
	}

	// Verify final values
	expectedValues := map[string]string{
		"key1": "value1_final",
		"key2": "value2_updated",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range expectedValues {
		if got := string(mergedData[k]); got != v {
			t.Errorf("Value mismatch for %s: got %v, want %v", k, got, v)
		}
	}
}

func TestCompactionManagerConcurrent(t *testing.T) {
	tempDir, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create compaction manager
	manager, err := NewCompactionManager(tempDir, 100*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Start compaction manager
	manager.Start()
	defer manager.Stop()

	// Create snapshots concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			data := map[string][]byte{
				fmt.Sprintf("key%d", i): []byte(fmt.Sprintf("value%d", i)),
			}
			createTestSnapshot(t, tempDir, data)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Wait for compaction to occur
	time.Sleep(200 * time.Millisecond)

	// Verify final state
	files, err := filepath.Glob(filepath.Join(tempDir, "snapshot_*.gob"))
	if err != nil {
		t.Fatalf("Failed to list snapshot files: %v", err)
	}

	if len(files) > 3 {
		t.Errorf("Too many snapshot files after compaction: got %d, want <= 3", len(files))
	}
}

func TestCompactionManagerStop(t *testing.T) {
	tempDir, cleanup := setupCompactionTest(t)
	defer cleanup()

	// Create compaction manager
	manager, err := NewCompactionManager(tempDir, 100*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Start compaction manager
	manager.Start()

	// Stop immediately
	manager.Stop()

	// Create some snapshots
	for i := 0; i < 5; i++ {
		data := map[string][]byte{
			fmt.Sprintf("key%d", i): []byte(fmt.Sprintf("value%d", i)),
		}
		createTestSnapshot(t, tempDir, data)
	}

	// Wait a bit to ensure no compaction occurs
	time.Sleep(200 * time.Millisecond)

	// Verify no compaction occurred
	files, err := filepath.Glob(filepath.Join(tempDir, "snapshot_*.gob"))
	if err != nil {
		t.Fatalf("Failed to list snapshot files: %v", err)
	}

	if len(files) != 5 {
		t.Errorf("Wrong number of snapshot files: got %d, want 5", len(files))
	}
}
