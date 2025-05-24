package integration

import (
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestNodeRestart(t *testing.T) {
	// Initialize controller
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

	// Define partitions
	partitions := []controller.Partition{
		{ID: 1, Leader: "node-1", Replicas: []string{"node-2", "node-3"}, Status: "healthy"},
		{ID: 2, Leader: "node-2", Replicas: []string{"node-1", "node-3"}, Status: "healthy"},
		{ID: 3, Leader: "node-3", Replicas: []string{"node-1", "node-2"}, Status: "healthy"},
	}

	// Register nodes
	nodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
		"node-3": "localhost:8083",
	}

	for nodeID, address := range nodes {
		err := helpers.RegisterNode(ctrl, nodeID, address)
		assert.NoError(t, err)
	}

	// Create partitions
	for _, partition := range partitions {
		err := helpers.CreatePartition(ctrl, partition)
		assert.NoError(t, err)
	}

	// Wait for nodes to become active
	time.Sleep(5 * time.Second)

	// Simulate restarting nodes one by one
	for nodeID := range nodes {
		// Remove node
		err := helpers.RemoveNode(ctrl, nodeID)
		assert.NoError(t, err)

		// Wait for failover to occur
		time.Sleep(10 * time.Second)

		// Re-register node
		err = helpers.RegisterNode(ctrl, nodeID, nodes[nodeID])
		assert.NoError(t, err)

		// Wait for node to become active with timeout
		timeout := time.After(10 * time.Second)
		tick := time.Tick(100 * time.Millisecond)
		for {
			select {
			case <-timeout:
				t.Fatalf("Timeout waiting for node %s to become active", nodeID)
			case <-tick:
				status, err := helpers.GetNodeStatus(ctrl, nodeID)
				if err == nil && status == "active" {
					goto NodeActive
				}
			}
		}
	NodeActive:

		// Wait for replication to catch up
		time.Sleep(5 * time.Second)

		// Verify that operations still work
		client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

		// Test Set operation
		err = client.Set("test-key", "test-value")
		assert.NoError(t, err)

		// Test Get operation
		value, err := client.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, "test-value", value)

		// Test Delete operation
		err = client.Delete("test-key")
		assert.NoError(t, err)

		// Verify deletion
		_, err = client.Get("test-key")
		assert.Error(t, err)
	}
}
