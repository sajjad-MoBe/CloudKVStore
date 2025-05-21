package storage

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// CompactionManager handles background compaction of snapshots
type CompactionManager struct {
	snapshotDir    string
	interval       time.Duration
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	lastCompaction time.Time
	maxSnapshots   int
}

// NewCompactionManager creates a new CompactionManager instance
func NewCompactionManager(snapshotDir string, interval time.Duration, maxSnapshots int) (*CompactionManager, error) {
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &CompactionManager{
		snapshotDir:    snapshotDir,
		interval:       interval,
		stopChan:       make(chan struct{}),
		lastCompaction: time.Now(),
		maxSnapshots:   maxSnapshots,
	}, nil
}

// Start begins the background compaction process
func (cm *CompactionManager) Start() {
	cm.wg.Add(1)
	go cm.compactionLoop()
}

// Stop gracefully stops the compaction process
func (cm *CompactionManager) Stop() {
	close(cm.stopChan)
	cm.wg.Wait()
}

// compactionLoop runs the periodic compaction process
func (cm *CompactionManager) compactionLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopChan:
			return
		case <-ticker.C:
			if err := cm.compact(); err != nil {
				fmt.Printf("Compaction failed: %v\n", err)
			}
		}
	}
}

// compact performs a single compaction cycle
func (cm *CompactionManager) compact() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// List all snapshot files
	files, err := filepath.Glob(filepath.Join(cm.snapshotDir, "snapshot_*.gob"))
	if err != nil {
		return fmt.Errorf("failed to list snapshot files: %w", err)
	}

	// If we have fewer snapshots than the maximum, no need to compact
	if len(files) <= cm.maxSnapshots {
		return nil
	}

	// Sort files by modification time (oldest first)
	sort.Slice(files, func(i, j int) bool {
		infoI, _ := os.Stat(files[i])
		infoJ, _ := os.Stat(files[j])
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	// Keep the most recent snapshot
	filesToMerge := files[:len(files)-1]

	// Merge all older snapshots into a single one
	mergedData, err := cm.mergeSnapshots(filesToMerge)
	if err != nil {
		return fmt.Errorf("failed to merge snapshots: %w", err)
	}

	// Create new merged snapshot
	timestamp := time.Now().UnixNano()
	mergedPath := filepath.Join(cm.snapshotDir, fmt.Sprintf("snapshot_%d_merged.gob", timestamp))

	file, err := os.Create(mergedPath)
	if err != nil {
		return fmt.Errorf("failed to create merged snapshot: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(mergedData); err != nil {
		os.Remove(mergedPath)
		return fmt.Errorf("failed to encode merged snapshot: %w", err)
	}

	// Remove old snapshots
	for _, file := range filesToMerge {
		if err := os.Remove(file); err != nil {
			fmt.Printf("Warning: failed to remove old snapshot %s: %v\n", file, err)
		}
	}

	cm.lastCompaction = time.Now()
	return nil
}

// mergeSnapshots merges multiple snapshots into a single data map
func (cm *CompactionManager) mergeSnapshots(files []string) (map[string][]byte, error) {
	mergedData := make(map[string][]byte)

	for _, file := range files {
		data, err := cm.loadSnapshot(file)
		if err != nil {
			return nil, fmt.Errorf("failed to load snapshot %s: %w", file, err)
		}

		// Merge data, newer values override older ones
		for k, v := range data {
			mergedData[k] = v
		}
	}

	return mergedData, nil
}

// loadSnapshot loads a single snapshot file
func (cm *CompactionManager) loadSnapshot(path string) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	var data map[string][]byte
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot data: %w", err)
	}

	return data, nil
}

// GetLastCompactionTime returns the time of the last successful compaction
func (cm *CompactionManager) GetLastCompactionTime() time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.lastCompaction
}
