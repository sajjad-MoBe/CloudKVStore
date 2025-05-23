package integration

import (
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"
	"github.com/stretchr/testify/assert"
)

func TestMultiNodeMultiShardSetup(t *testing.T) {
	// Initialize controller
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

	// Setup mock nodes
	mockNodes := map[string]*helpers.MockNode{
		"node-1": helpers.NewMockNode("localhost:8081"),
		"node-2": helpers.NewMockNode("localhost:8082"),
		"node-3": helpers.NewMockNode("localhost:8083"),
		"node-4": helpers.NewMockNode("localhost:8084"),
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

	// Define multiple partitions
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
	time.Sleep(2 * time.Second)

	// Measure RPS with initial setup
	initialRPS := helpers.MeasureMixedOperationsRPS(helpers.NewTestClient("http://localhost"+ctrl.GetTestPort()), 1000)
	shared.DefaultLogger.Info("Initial RPS: %f", initialRPS)

	// Register a new node before updating replicas
	err := helpers.RegisterNode(ctrl, "node-4", "localhost:8084")
	assert.NoError(t, err)

	// Wait for node-4 to become active
	time.Sleep(5 * time.Second)

	// Add more replicas to each partition
	for _, partition := range partitions {
		partition.Replicas = append(partition.Replicas, "node-4")
		err := helpers.UpdatePartitionReplicas(ctrl, partition.ID, partition.Replicas)
		assert.NoError(t, err)
	}

	// Wait for rebalancing to complete
	time.Sleep(10 * time.Second)

	// Measure RPS after adding more replicas
	finalRPS := helpers.MeasureMixedOperationsRPS(helpers.NewTestClient("http://localhost"+ctrl.GetTestPort()), 1000)
	shared.DefaultLogger.Info("Final RPS: %f", finalRPS)

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
