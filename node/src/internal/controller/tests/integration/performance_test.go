package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestSetPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Measure Set RPS
	iterations := 1000
	setRPS := helpers.MeasureOperationRPS(client, "set", iterations)
	t.Logf("Set RPS: %f", setRPS)

	// Verify minimum performance threshold
	assert.Greater(t, setRPS, 100.0, "Set operations should achieve at least 100 RPS")
}

func TestGetPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// First set some values
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		value := fmt.Sprintf("perf-value-%d", i)
		err := client.Set(key, value)
		assert.NoError(t, err)
	}

	// Measure Get RPS
	iterations := 1000
	getRPS := helpers.MeasureOperationRPS(client, "get", iterations)
	t.Logf("Get RPS: %f", getRPS)

	// Verify minimum performance threshold
	assert.Greater(t, getRPS, 200.0, "Get operations should achieve at least 200 RPS")
}

func TestDeletePerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// First set some values
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("perf-key-%d", i)
		value := fmt.Sprintf("perf-value-%d", i)
		err := client.Set(key, value)
		assert.NoError(t, err)
	}

	// Measure Delete RPS
	iterations := 1000
	deleteRPS := helpers.MeasureOperationRPS(client, "delete", iterations)
	t.Logf("Delete RPS: %f", deleteRPS)

	// Verify minimum performance threshold
	assert.Greater(t, deleteRPS, 150.0, "Delete operations should achieve at least 150 RPS")
}

func TestMixedOperationsPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Measure mixed operations RPS
	iterations := 1000
	mixedRPS := helpers.MeasureMixedOperationsRPS(client, iterations)
	t.Logf("Mixed Operations RPS: %f", mixedRPS)

	// Verify minimum performance threshold
	assert.Greater(t, mixedRPS, 50.0, "Mixed operations should achieve at least 50 RPS")
}

func TestLoadTesting(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")

	// Create multiple clients
	numClients := 10
	clients := make([]*helpers.TestClient, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())
	}

	// Create a channel to signal completion
	done := make(chan bool)

	// Launch multiple clients to perform operations
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			client := clients[clientID]
			iterations := 100

			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("load-key-%d-%d", clientID, j)
				value := fmt.Sprintf("load-value-%d-%d", clientID, j)

				// Perform operations
				err := client.Set(key, value)
				assert.NoError(t, err)

				_, err = client.Get(key)
				assert.NoError(t, err)

				err = client.Delete(key)
				assert.NoError(t, err)
			}

			done <- true
		}(i)
	}

	// Wait for all clients to complete
	for i := 0; i < numClients; i++ {
		<-done
	}

	// Measure overall performance
	start := time.Now()
	iterations := 1000
	mixedRPS := helpers.MeasureMixedOperationsRPS(clients[0], iterations)
	duration := time.Since(start)

	t.Logf("Load Test Results:")
	t.Logf("Total Operations: %d", iterations*3)
	t.Logf("Total Duration: %v", duration)
	t.Logf("Overall RPS: %f", mixedRPS)
}
