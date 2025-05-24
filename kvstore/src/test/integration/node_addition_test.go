package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/partition"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/wal"
)

func TestNodeAdditionUnderLoad(t *testing.T) {
	// Setup controller with unique ports
	controllerPort := 9190
	baseNodePort := 9191
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

	// Start with two nodes
	initialNodes := startNodes(t, 2, baseNodePort, controllerPort)
	defer func() {
		for _, n := range initialNodes {
			if n != nil {
				n.node.Stop()
			}
		}
		ctrl.Stop()
		close(controllerStopCh)
		time.Sleep(time.Second)
	}()

	// Register initial nodes with controller
	for i, n := range initialNodes {
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
	waitForNodesActive(t, controllerPort, 2)

	// Create initial partitions
	createInitialPartitions(t, controllerPort)

	// Start load generation
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		generateLoad(t, initialNodes, stopLoad)
	}()

	// Wait for some load to be generated
	time.Sleep(5 * time.Second)

	// Add a new node
	newNode := startNodes(t, 1, baseNodePort+2, controllerPort)[0]
	defer func() {
		if newNode != nil {
			newNode.node.Stop()
		}
	}()

	// Register the new node
	nodeID := "node-3"
	address := fmt.Sprintf("localhost:%s", newNode.port)

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
		t.Fatalf("Failed to register new node: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Failed to register new node: unexpected status code %d", resp.StatusCode)
	}

	// Wait for the new node to become active with a longer timeout
	timeout := time.After(10 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for new node to become active")
		case <-tick:
			url := fmt.Sprintf("http://localhost:%d/nodes/%s/status", controllerPort, nodeID)
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			var result struct {
				Status string `json:"status"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				resp.Body.Close()
				continue
			}
			resp.Body.Close()
			if result.Status == "active" {
				goto NodeActive
			}
		}
	}
NodeActive:

	// Wait for partition rebalancing
	time.Sleep(5 * time.Second)

	// Verify operations are still working
	t.Run("Verify operations after node addition", func(t *testing.T) {
		// Test set operation
		err := setValueOnNode(initialNodes[0].port, "test-key-after", "test-value-after")
		if err != nil {
			t.Fatalf("Failed to set value after node addition: %v", err)
		}

		// Test get operation
		value, err := getValueFromNode(initialNodes[0].port, "test-key-after")
		if err != nil {
			t.Fatalf("Failed to get value after node addition: %v", err)
		}
		if value != "test-value-after" {
			t.Errorf("Expected value 'test-value-after', got '%s'", value)
		}

		// Test delete operation
		err = deleteValueOnNode(initialNodes[0].port, "test-key-after")
		if err != nil {
			t.Fatalf("Failed to delete value after node addition: %v", err)
		}

		// Verify deletion
		_, err = getValueFromNode(initialNodes[0].port, "test-key-after")
		if err == nil {
			t.Error("Expected error when getting deleted value")
		}
	})

	// Stop load generation
	close(stopLoad)
	wg.Wait()
}

func waitForNodesActive(t *testing.T, controllerPort, expectedNodes int) {
	timeout := time.After(10 * time.Second) // Increased timeout
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for nodes to become active")
		case <-tick:
			activeNodes := 0
			for i := 0; i < expectedNodes; i++ {
				nodeID := fmt.Sprintf("node-%d", i+1)
				url := fmt.Sprintf("http://localhost:%d/nodes/%s/status", controllerPort, nodeID)
				resp, err := http.Get(url)
				if err != nil {
					continue
				}
				var result struct {
					Status string `json:"status"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					resp.Body.Close()
					continue
				}
				resp.Body.Close()
				if result.Status == "active" {
					activeNodes++
				}
			}
			if activeNodes == expectedNodes {
				return
			}
		}
	}
}

func createInitialPartitions(t *testing.T, controllerPort int) {
	nodeIDs := []string{"node-1", "node-2"}
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

	time.Sleep(2 * time.Second)
}

func generateLoad(t *testing.T, nodes []*testNode, stopCh chan struct{}) {
	operationCount := 0
	for {
		select {
		case <-stopCh:
			t.Logf("Load generation completed. Total operations: %d", operationCount)
			return
		default:
			// Select a random node
			node := nodes[operationCount%len(nodes)]

			// Generate a unique key
			key := fmt.Sprintf("load-key-%d", operationCount)
			value := fmt.Sprintf("load-value-%d", operationCount)

			// Perform set operation
			err := setValueOnNode(node.port, key, value)
			if err != nil {
				t.Logf("Set operation failed: %v", err)
				continue
			}

			// Perform get operation
			gotValue, err := getValueFromNode(node.port, key)
			if err != nil {
				t.Logf("Get operation failed: %v", err)
				continue
			}

			if gotValue != value {
				t.Logf("Value mismatch: expected %s, got %s", value, gotValue)
				continue
			}

			// Perform delete operation
			err = deleteValueOnNode(node.port, key)
			if err != nil {
				t.Logf("Delete operation failed: %v", err)
				continue
			}

			operationCount++
			time.Sleep(10 * time.Millisecond) // Add small delay to prevent overwhelming the system
		}
	}
}
