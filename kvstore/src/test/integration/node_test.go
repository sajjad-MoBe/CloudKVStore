package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/cmd/node"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

const (
	controllerPort = "8080"
	nodePort       = "8081"
	controllerURL  = "http://localhost:" + controllerPort
	nodeURL        = "http://localhost:" + nodePort
)

func TestNodeOperations(t *testing.T) {
	// Start controller
	config := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		WALConfig: wal.WALConfig{
			MaxFileSize: 10 * 1024 * 1024, // 10MB
		},
	}
	healthManager := shared.NewHealthManager("controller-1")
	partitionManager := partition.NewPartitionManager(config, healthManager)
	ctrl := controller.NewController(partitionManager, healthManager)

	// Start controller in a goroutine
	go func() {
		if err := ctrl.Start(":" + controllerPort); err != nil {
			t.Errorf("Failed to start controller: %v", err)
		}
	}()

	// Give controller time to start
	time.Sleep(time.Second)

	// Start node
	n := node.NewNode("node-1", ":"+nodePort, controllerURL)
	go func() {
		if err := n.Start(); err != nil {
			t.Errorf("Failed to start node: %v", err)
		}
	}()

	// Give node time to start
	time.Sleep(time.Second)

	// Test basic operations
	t.Run("Basic Operations", func(t *testing.T) {
		// Test Set
		key := "test-key"
		value := "test-value"
		if err := setValue(key, value); err != nil {
			t.Errorf("Set operation failed: %v", err)
		}

		// Test Get
		gotValue, err := getValue(key)
		if err != nil {
			t.Errorf("Get operation failed: %v", err)
		}
		if gotValue != value {
			t.Errorf("Get returned wrong value: got %v, want %v", gotValue, value)
		}

		// Test Delete
		if err := deleteValue(key); err != nil {
			t.Errorf("Delete operation failed: %v", err)
		}

		// Verify deletion
		_, err = getValue(key)
		if err == nil {
			t.Error("Key still exists after deletion")
		}
	})

	// Test performance
	t.Run("Performance Test", func(t *testing.T) {
		// Measure Set operations
		setRPS := measureSetOperations(t, 1000)
		t.Logf("Set operations: %.2f RPS", setRPS)

		// Measure Get operations
		getRPS := measureGetOperations(t, 1000)
		t.Logf("Get operations: %.2f RPS", getRPS)

		// Measure Delete operations
		deleteRPS := measureDeleteOperations(t, 1000)
		t.Logf("Delete operations: %.2f RPS", deleteRPS)
	})

	// Cleanup
	n.Stop()
	ctrl.Stop()
}

// Helper functions for HTTP operations
func setValue(key, value string) error {
	data := map[string]string{"value": value}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/kv/%s", nodeURL, key), bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func getValue(key string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/kv/%s", nodeURL, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result["value"], nil
}

func deleteValue(key string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/kv/%s", nodeURL, key), nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// Performance measurement functions
func measureSetOperations(t *testing.T, count int) float64 {
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		value := fmt.Sprintf("perf-value-%d", i)
		if err := setValue(key, value); err != nil {
			t.Errorf("Set operation failed: %v", err)
			return 0
		}
	}
	duration := time.Since(start)
	return float64(count) / duration.Seconds()
}

func measureGetOperations(t *testing.T, count int) float64 {
	// First set some values
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		value := fmt.Sprintf("perf-value-%d", i)
		if err := setValue(key, value); err != nil {
			t.Errorf("Set operation failed: %v", err)
			return 0
		}
	}

	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		if _, err := getValue(key); err != nil {
			t.Errorf("Get operation failed: %v", err)
			return 0
		}
	}
	duration := time.Since(start)
	return float64(count) / duration.Seconds()
}

func measureDeleteOperations(t *testing.T, count int) float64 {
	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		if err := deleteValue(key); err != nil {
			t.Errorf("Delete operation failed: %v", err)
			return 0
		}
	}
	duration := time.Since(start)
	return float64(count) / duration.Seconds()
}
