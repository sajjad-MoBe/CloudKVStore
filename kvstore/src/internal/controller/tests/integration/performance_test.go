package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller/tests/helpers"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/stretchr/testify/assert"
)

func TestSetPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	shared.DefaultLogger.Info("Starting Set performance test...")
	iterations := 1000
	rps := helpers.MeasureOperationRPS(client, "set", iterations)
	shared.DefaultLogger.Info("Set RPS: %f", rps)
}

func TestGetPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	shared.DefaultLogger.Info("Setting up test data for Get performance test...")
	// First set up some test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)
		err := client.Set(key, value)
		assert.NoError(t, err)
	}

	shared.DefaultLogger.Info("Starting Get performance test...")
	iterations := 1000
	rps := helpers.MeasureOperationRPS(client, "get", iterations)
	shared.DefaultLogger.Info("Get RPS: %f", rps)
}

func TestDeletePerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	shared.DefaultLogger.Info("Setting up test data for Delete performance test...")
	// First set up some test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		value := fmt.Sprintf("test-value-%d", i)
		err := client.Set(key, value)
		assert.NoError(t, err)
	}

	shared.DefaultLogger.Info("Starting Delete performance test...")
	iterations := 1000
	rps := helpers.MeasureOperationRPS(client, "delete", iterations)
	shared.DefaultLogger.Info("Delete RPS: %f", rps)
}

func TestMixedOperationsPerformance(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	shared.DefaultLogger.Info("Starting Mixed Operations performance test...")
	iterations := 1000
	rps := helpers.MeasureMixedOperationsRPS(client, iterations)
	shared.DefaultLogger.Info("Mixed Operations RPS: %f", rps)
}

func TestLoadTesting(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	shared.DefaultLogger.Info("Starting Load Test...")
	start := time.Now()
	totalOperations := 3000
	errors := 0

	// Perform a mix of operations
	for i := 0; i < totalOperations; i++ {
		key := fmt.Sprintf("load-test-key-%d", i)
		value := fmt.Sprintf("load-test-value-%d", i)

		// Set operation
		err := client.Set(key, value)
		if err != nil {
			shared.DefaultLogger.Error("Error during set operation: %v", err)
			errors++
			continue
		}

		// Get operation
		retrievedValue, err := client.Get(key)
		if err != nil {
			shared.DefaultLogger.Error("Error during get operation: %v", err)
			errors++
			continue
		}
		if retrievedValue != value {
			shared.DefaultLogger.Error("Value mismatch: expected %s, got %s", value, retrievedValue)
			errors++
			continue
		}

		// Delete operation
		err = client.Delete(key)
		if err != nil {
			shared.DefaultLogger.Error("Error during delete operation: %v", err)
			errors++
			continue
		}
	}

	duration := time.Since(start)
	overallRPS := float64(totalOperations) / duration.Seconds()

	shared.DefaultLogger.Info("Load Test Results:")
	shared.DefaultLogger.Info("Total Operations: %d", totalOperations)
	shared.DefaultLogger.Info("Total Duration: %v", duration)
	shared.DefaultLogger.Info("Overall RPS: %f", overallRPS)
	shared.DefaultLogger.Info("Total Errors: %d", errors)

	// Allow some errors during load testing
	assert.Less(t, errors, totalOperations/10, "Too many errors during load test")
}
