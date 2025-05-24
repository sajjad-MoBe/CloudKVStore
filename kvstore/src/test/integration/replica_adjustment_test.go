package integration

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller"
	helpers "github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller/tests/helpers"
)

func TestReplicaAdjustment(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	basePort := 9000

	// Register initial nodes
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	for i, nodeID := range nodeIDs {
		address := fmt.Sprintf("localhost:%d", basePort+i)
		err := helpers.RegisterNode(ctrl, nodeID, address)
		if err != nil {
			t.Fatalf("Failed to register node %s: %v", nodeID, err)
		}
	}
	// Wait for nodes to become active
	time.Sleep(2 * time.Second)

	// Create initial partition with 2 replicas
	partition := controller.Partition{
		ID:       1,
		Leader:   nodeIDs[0],
		Replicas: nodeIDs[1:3],
		Status:   "active",
	}
	err := helpers.CreatePartition(ctrl, partition)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}
	// Wait for partition to be created
	time.Sleep(2 * time.Second)

	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Generate and insert test data
	testData := generateTestData(100)
	for key, value := range testData {
		err := client.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to put test data: %v", err)
		}
	}

	t.Run("Initial Data Consistency", func(t *testing.T) {
		verifyDataConsistency(t, client, testData)
	})

	t.Run("Increase Replicas", func(t *testing.T) {
		// Add a new node
		newNodeID := "node-4"
		newNodeAddr := fmt.Sprintf("localhost:%d", basePort+3)
		err := helpers.RegisterNode(ctrl, newNodeID, newNodeAddr)
		if err != nil {
			t.Fatalf("Failed to register new node: %v", err)
		}
		// Increase replicas to 3
		replicas := []string{"node-2", "node-3", newNodeID}
		err = helpers.UpdatePartitionReplicas(ctrl, partition.ID, replicas)
		if err != nil {
			t.Fatalf("Failed to increase replicas: %v", err)
		}
		time.Sleep(5 * time.Second)
		verifyDataConsistency(t, client, testData)
	})

	t.Run("Decrease Replicas", func(t *testing.T) {
		// Decrease replicas to 2
		replicas := []string{"node-2", "node-3"}
		err := helpers.UpdatePartitionReplicas(ctrl, partition.ID, replicas)
		if err != nil {
			t.Fatalf("Failed to decrease replicas: %v", err)
		}
		time.Sleep(5 * time.Second)
		verifyDataConsistency(t, client, testData)
	})
}

func verifyDataConsistency(t *testing.T, client *helpers.TestClient, expectedData map[string]string) {
	for key, expectedValue := range expectedData {
		value, err := client.Get(key)
		if err != nil {
			t.Errorf("Failed to get value for key %s: %v", key, err)
			continue
		}
		if value != expectedValue {
			t.Errorf("Value mismatch for key %s: expected=%s, got=%s", key, expectedValue, value)
		}
	}
}

func generateTestData(count int) map[string]string {
	data := make(map[string]string)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", rand.Intn(1000))
		data[key] = value
	}
	return data
}
