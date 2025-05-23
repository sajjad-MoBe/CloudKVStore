package integration

import (
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestNodeSetup(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl)

	nodeID := "test-node-1"
	address := "localhost:8080"
	err := helpers.RegisterNode(ctrl, nodeID, address)
	assert.NoError(t, err)

	// Wait for node to become active with timeout
	timeout := time.After(5 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for node to become active")
		case <-tick:
			status, err := helpers.GetNodeStatus(ctrl, nodeID)
			if err == nil && status == "active" {
				goto NodeActive
			}
		}
	}
NodeActive:

	// Load balancer config check
	lbConfig, err := helpers.GetLoadBalancerConfig(ctrl)
	assert.NoError(t, err)
	assert.NotNil(t, lbConfig)
}

func TestNodeRegistration(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")

	// Test node registration
	nodeID := "test-node-1"
	address := "localhost:8080"
	err := helpers.RegisterNode(ctrl, nodeID, address)
	assert.NoError(t, err)

	// Verify node is registered
	nodes, err := helpers.GetNodes(ctrl)
	assert.NoError(t, err)
	assert.Contains(t, nodes, nodeID)
	assert.Equal(t, address, nodes[nodeID])
}

func TestNodeHealthCheck(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")

	// Register a test node
	nodeID := "test-node-1"
	address := "localhost:8080"
	err := helpers.RegisterNode(ctrl, nodeID, address)
	assert.NoError(t, err)

	// Wait for node to become active with timeout
	timeout := time.After(5 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for node to become active")
		case <-tick:
			status, err := helpers.GetNodeStatus(ctrl, nodeID)
			if err == nil && status == "active" {
				goto NodeActive
			}
		}
	}
NodeActive:
}

func TestNodeRemoval(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")

	// Register a test node
	nodeID := "test-node-1"
	address := "localhost:8080"
	err := helpers.RegisterNode(ctrl, nodeID, address)
	assert.NoError(t, err)

	// Remove the node
	err = helpers.RemoveNode(ctrl, nodeID)
	assert.NoError(t, err)

	// Verify node is removed
	nodes, err := helpers.GetNodes(ctrl)
	assert.NoError(t, err)
	assert.NotContains(t, nodes, nodeID)
}

func TestLoadBalancerIntegration(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")

	// Register multiple nodes
	nodes := map[string]string{
		"node-1": "localhost:8081",
		"node-2": "localhost:8082",
		"node-3": "localhost:8083",
	}

	for nodeID, address := range nodes {
		err := helpers.RegisterNode(ctrl, nodeID, address)
		assert.NoError(t, err)
	}

	// Wait for all nodes to become active with timeout
	timeout := time.After(5 * time.Second)
	tick := time.Tick(100 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for nodes to become active")
		case <-tick:
			allActive := true
			for nodeID := range nodes {
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

	// Verify all nodes are registered and active
	registeredNodes, err := helpers.GetNodes(ctrl)
	assert.NoError(t, err)
	assert.Equal(t, len(nodes), len(registeredNodes))

	for nodeID := range nodes {
		status, err := helpers.GetNodeStatus(ctrl, nodeID)
		assert.NoError(t, err)
		assert.Equal(t, "active", status)
	}
}
