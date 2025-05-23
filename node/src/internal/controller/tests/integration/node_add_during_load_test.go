package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestNodeAddDuringLoad(t *testing.T) {
	// Initialize controller
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

	// Define initial partitions
	partitions := []controller.Partition{
		{ID: 1, Leader: "node-1", Replicas: []string{"node-2"}, Status: "healthy"},
		{ID: 2, Leader: "node-2", Replicas: []string{"node-1"}, Status: "healthy"},
	}

	// Register initial nodes
	nodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
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

	// Simulate load by performing operations
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Perform Set operations under load
	for i := 0; i < 10; i++ {
		err := client.Set("test-key-"+strconv.Itoa(i), "test-value-"+strconv.Itoa(i))
		assert.NoError(t, err)
	}

	// Add a new node during load
	newNodeID := "node-3"
	newNodeAddress := "localhost:8083"
	err := helpers.RegisterNode(ctrl, newNodeID, newNodeAddress)
	assert.NoError(t, err)

	// Wait for rebalancing to occur
	time.Sleep(10 * time.Second)

	// Verify that operations still work
	for i := 0; i < 10; i++ {
		value, err := client.Get("test-key-" + strconv.Itoa(i))
		assert.NoError(t, err)
		assert.Equal(t, "test-value-"+strconv.Itoa(i), value)
	}
}
