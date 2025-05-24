package integration

import (
	"fmt"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/controller/tests/helpers"
	"github.com/sajjad-MoBe/CloudKVStore/kvstore/src/internal/shared"
	"github.com/stretchr/testify/assert"
)

var logger = shared.DefaultLogger

func TestBasicOperations(t *testing.T) {
	// Setup controller and client
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Test Set operation
	logger.Info("Testing Set operation...")
	err := client.Set("test-key", "test-value")
	assert.NoError(t, err)
	logger.Info("Set operation completed successfully")

	// Test Get operation
	logger.Info("Testing Get operation...")
	value, err := client.Get("test-key")
	assert.NoError(t, err)
	assert.Equal(t, "test-value", value)
	logger.Info("Get operation completed successfully, retrieved value: %s", value)

	// Test Delete operation
	logger.Info("Testing Delete operation...")
	err = client.Delete("test-key")
	assert.NoError(t, err)
	logger.Info("Delete operation completed successfully")

	// Verify deletion
	logger.Info("Verifying deletion...")
	_, err = client.Get("test-key")
	assert.Error(t, err)
	logger.Info("Deletion verified successfully")
}

func TestConcurrentOperations(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	logger.Info("Starting concurrent operations test...")
	// Create a channel to signal completion
	done := make(chan bool)
	errors := make(chan error, 10)

	// Launch multiple goroutines to perform operations concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent-key-%d", id)
			value := fmt.Sprintf("concurrent-value-%d", id)

			logger.Info("Goroutine %d: Setting value for key %s", id, key)
			// Set value
			err := client.Set(key, value)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: set error: %v", id, err)
				done <- true
				return
			}

			logger.Info("Goroutine %d: Getting value for key %s", id, key)
			// Get value
			retrievedValue, err := client.Get(key)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: get error: %v", id, err)
				done <- true
				return
			}
			if retrievedValue != value {
				errors <- fmt.Errorf("goroutine %d: value mismatch: expected %s, got %s", id, value, retrievedValue)
				done <- true
				return
			}

			logger.Info("Goroutine %d: Deleting key %s", id, key)
			// Delete value
			err = client.Delete(key)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: delete error: %v", id, err)
				done <- true
				return
			}

			logger.Info("Goroutine %d: Completed all operations successfully", id)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Check for any errors
	close(errors)
	for err := range errors {
		logger.Error("Concurrent operation error: %v", err)
		t.Error(err)
	}
	logger.Info("Concurrent operations test completed")
}

func TestErrorCases(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	logger.Info("Testing error cases...")

	// Test getting non-existent key
	logger.Info("Testing Get for non-existent key...")
	_, err := client.Get("non-existent-key")
	assert.Error(t, err)
	logger.Info("Get for non-existent key failed as expected")

	// Test deleting non-existent key
	logger.Info("Testing Delete for non-existent key...")
	err = client.Delete("non-existent-key")
	assert.Error(t, err)
	logger.Info("Delete for non-existent key failed as expected")

	// Test setting empty key
	logger.Info("Testing Set with empty key...")
	err = client.Set("", "value")
	assert.Error(t, err)
	logger.Info("Set with empty key failed as expected")

	// Test setting empty value
	logger.Info("Testing Set with empty value...")
	err = client.Set("key", "")
	assert.Error(t, err)
	logger.Info("Set with empty value failed as expected")

	logger.Info("Error cases test completed")
}

func TestOperationVerification(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	logger.Info("Starting operation verification test...")

	// Set a value
	key := "verification-key"
	value := "verification-value"
	logger.Info("Setting value for key %s", key)
	err := client.Set(key, value)
	assert.NoError(t, err)
	logger.Info("Set operation completed successfully")

	// Verify the value was set correctly
	logger.Info("Verifying set value...")
	retrievedValue, err := client.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
	logger.Info("Value verification successful: %s", retrievedValue)

	// Delete the value
	logger.Info("Deleting value...")
	err = client.Delete(key)
	assert.NoError(t, err)
	logger.Info("Delete operation completed successfully")

	// Verify the value was deleted
	logger.Info("Verifying deletion...")
	_, err = client.Get(key)
	assert.Error(t, err)
	logger.Info("Deletion verification successful")

	logger.Info("Operation verification test completed")
}
