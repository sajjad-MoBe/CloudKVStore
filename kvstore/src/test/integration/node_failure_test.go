package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

func TestNodeFailureAndFailover(t *testing.T) {
	// Setup controller with unique ports
	controllerPort := 9090
	baseNodePort := 9091
	config := partition.PartitionConfig{
		MaxMemTableSize: 1024 * 1024, // 1MB
		WALConfig: wal.WALConfig{
			MaxFileSize: 10 * 1024 * 1024, // 10MB
		},
	}
	healthManager := shared.NewHealthManager("controller-1")
	partitionManager := partition.NewPartitionManager(config, healthManager)
	ctrl := controller.NewController(partitionManager, healthManager)

	controllerStopCh := make(chan struct{})
	go func() {
		if err := ctrl.Start(fmt.Sprintf(":%d", controllerPort)); err != nil {
			t.Errorf("Failed to start controller: %v", err)
		}
	}()

	time.Sleep(time.Second)

	// Start three nodes
	nodes := startNodes(t, 3, baseNodePort, controllerPort)
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.node.Stop()
			}
		}
		ctrl.Stop()
		close(controllerStopCh)
		time.Sleep(time.Second)
	}()

	// Register nodes with controller
	for i, n := range nodes {
		nodeID := fmt.Sprintf("node-%d", i+1)
		address := fmt.Sprintf("localhost:%s", n.port)

		req := struct {
			ID      string `json:"id"`
			Address string `json:"address"`
		}{
			ID:      nodeID,
			Address: address,
		}

		jsonData, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("Failed to marshal node registration request: %v", err)
		}

		url := fmt.Sprintf("http://localhost:%d/nodes", controllerPort)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			t.Fatalf("Failed to register node: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("Failed to register node: unexpected status code %d", resp.StatusCode)
		}
	}

	// Wait for nodes to become active
	timeout := time.After(5 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for nodes to become active")
		case <-tick:
			allActive := true
			for i := range nodes {
				nodeID := fmt.Sprintf("node-%d", i+1)
				url := fmt.Sprintf("http://localhost:%d/nodes/%s/status", controllerPort, nodeID)
				resp, err := http.Get(url)
				if err != nil {
					allActive = false
					continue
				}
				var result struct {
					Status string `json:"status"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					resp.Body.Close()
					allActive = false
					continue
				}
				resp.Body.Close()
				if result.Status != "active" {
					allActive = false
					continue
				}
			}
			if allActive {
				goto NodesActive
			}
		}
	}
NodesActive:

	// Create a partition with all three nodes
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	req := struct {
		ID       int      `json:"id"`
		Leader   string   `json:"leader"`
		Replicas []string `json:"replicas"`
	}{
		ID:       1,
		Leader:   nodeIDs[0],
		Replicas: nodeIDs[1:],
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal partition creation request: %v", err)
	}

	url := fmt.Sprintf("http://localhost:%d/partitions", controllerPort)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Failed to create partition: unexpected status code %d", resp.StatusCode)
	}

	// Wait for partition to be created
	time.Sleep(2 * time.Second)

	// Test 1: Basic operations before node failure
	t.Run("Basic operations before failure", func(t *testing.T) {
		// Set a value
		err := setValueOnNode(nodes[0].port, "test-key", "test-value")
		if err != nil {
			t.Fatalf("Failed to set value: %v", err)
		}

		// Get the value
		value, err := getValueFromNode(nodes[0].port, "test-key")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		if value != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", value)
		}
	})

	// Test 2: Node failure and failover
	t.Run("Node failure and failover", func(t *testing.T) {
		// Stop the leader node (node-1)
		nodes[0].node.Stop()
		nodes[0] = nil // Mark as stopped

		// Wait for failover
		time.Sleep(5 * time.Second)

		// Verify the partition has a new leader
		url := fmt.Sprintf("http://localhost:%d/partitions/1", controllerPort)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Failed to get partition status: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to get partition status: unexpected status code %d", resp.StatusCode)
		}

		var partitionStatus struct {
			Leader string `json:"leader"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&partitionStatus); err != nil {
			t.Fatalf("Failed to decode partition status: %v", err)
		}

		if partitionStatus.Leader == "node-1" {
			t.Error("Expected leader to change after node failure")
		}

		// Test operations with the new leader
		// Set a new value
		err = setValueOnNode(nodes[1].port, "test-key-2", "test-value-2")
		if err != nil {
			t.Fatalf("Failed to set value after failover: %v", err)
		}

		// Get the value
		value, err := getValueFromNode(nodes[1].port, "test-key-2")
		if err != nil {
			t.Fatalf("Failed to get value after failover: %v", err)
		}
		if value != "test-value-2" {
			t.Errorf("Expected value 'test-value-2', got '%s'", value)
		}

		// Delete the value
		err = deleteValueOnNode(nodes[1].port, "test-key-2")
		if err != nil {
			t.Fatalf("Failed to delete value after failover: %v", err)
		}

		// Verify deletion
		_, err = getValueFromNode(nodes[1].port, "test-key-2")
		if err == nil {
			t.Error("Expected error when getting deleted value")
		}
	})
}
