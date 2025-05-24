package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestNodeAddDuringLoad(t *testing.T) {
	// Setup initial controller
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

	// Setup mock nodes
	mockNodes := map[string]*helpers.MockNode{
		"node-1": helpers.NewMockNode("localhost:8081"),
		"node-2": helpers.NewMockNode("localhost:8082"),
		"node-3": helpers.NewMockNode("localhost:8083"),
	}

	// Start mock nodes
	for _, node := range mockNodes {
		go node.Start()
	}
	defer func() {
		for _, node := range mockNodes {
			node.Stop()
		}
	}()

	// Wait for mock nodes to start
	time.Sleep(100 * time.Millisecond)

	// Setup initial nodes
	initialNodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
	}

	// Register initial nodes
	for nodeID, address := range initialNodes {
		err := helpers.RegisterNode(ctrl, nodeID, address)
		assert.NoError(t, err)
	}

	// Wait for initial nodes to become active
	timeout := time.After(5 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for initial nodes to become active")
		case <-tick:
			allActive := true
			for nodeID := range initialNodes {
				status, err := helpers.GetNodeStatus(ctrl, nodeID)
				if err != nil || status != "active" {
					allActive = false
					break
				}
			}
			if allActive {
				goto InitialNodesActive
			}
		}
	}
InitialNodesActive:

	// Create some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	// Create initial partitions
	initialPartitions := []controller.Partition{
		{ID: 1, Leader: "node-1", Replicas: []string{"node-2"}, Status: "healthy"},
		{ID: 2, Leader: "node-2", Replicas: []string{"node-1"}, Status: "healthy"},
	}

	for _, partition := range initialPartitions {
		err := helpers.CreatePartition(ctrl, partition)
		assert.NoError(t, err)
	}

	// Wait for partitions to be initialized
	time.Sleep(1 * time.Second)

	// Start background load
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	loadErrors := make(chan error, 100)

	// Start multiple goroutines to simulate load
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Perform random operations
					for key, value := range testData {
						// Set operation
						err := helpers.SetValue(ctrl, key, value)
						if err != nil {
							select {
							case loadErrors <- err:
							default:
							}
							continue
						}

						// Get operation
						gotValue, err := helpers.GetValue(ctrl, key)
						if err != nil {
							select {
							case loadErrors <- err:
							default:
							}
							continue
						}
						if gotValue != value {
							select {
							case loadErrors <- fmt.Errorf("value mismatch for key %s: expected %s, got %s", key, value, gotValue):
							default:
							}
						}
					}
					time.Sleep(100 * time.Millisecond) // Prevent overwhelming the system
				}
			}
		}()
	}

	// Add new node during load
	newNodeID := "node-3"
	newNodeAddress := "localhost:8083"
	err := helpers.RegisterNode(ctrl, newNodeID, newNodeAddress)
	assert.NoError(t, err)

	// Wait for new node to become active
	timeout = time.After(5 * time.Second)
	tick = time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for new node to become active")
		case <-tick:
			status, err := helpers.GetNodeStatus(ctrl, newNodeID)
			if err == nil && status == "active" {
				goto NewNodeActive
			}
		}
	}
NewNodeActive:

	// Verify all nodes are active after rebalancing
	allNodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
		"node-3": "localhost:8083",
	}

	timeout = time.After(5 * time.Second)
	tick = time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for all nodes to become active after rebalancing")
		case <-tick:
			allActive := true
			for nodeID := range allNodes {
				status, err := helpers.GetNodeStatus(ctrl, nodeID)
				if err != nil || status != "active" {
					allActive = false
					break
				}
			}
			if allActive {
				goto AllNodesActive
			}
		}
	}
AllNodesActive:

	// Continue load for a while to ensure stability
	time.Sleep(2 * time.Second)

	// Stop the load
	cancel()
	wg.Wait()

	// Check for any errors during load
	close(loadErrors)
	var loadErrorCount int
	for err := range loadErrors {
		t.Logf("Error during load: %v", err)
		loadErrorCount++
	}
	assert.Equal(t, 0, loadErrorCount, "Expected no errors during load")

	// Verify data consistency after rebalancing
	for key, expectedValue := range testData {
		gotValue, err := helpers.GetValue(ctrl, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, gotValue, "Data consistency check failed for key %s", key)
	}

	// Verify partition distribution
	var currentPartitions []*controller.Partition
	currentPartitions, err = helpers.GetPartitions(ctrl)
	assert.NoError(t, err)
	assert.Greater(t, len(currentPartitions), 0, "Expected at least one partition")

	// Verify that the new node has some partitions
	hasPartitions := false
	for _, partition := range currentPartitions {
		if partition.Leader == newNodeID || contains(partition.Replicas, newNodeID) {
			hasPartitions = true
			break
		}
	}
	assert.True(t, hasPartitions, "New node should have some partitions after rebalancing")
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
