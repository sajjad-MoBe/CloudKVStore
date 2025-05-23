package integration

import (
	"fmt"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/controller/tests/helpers"
	"github.com/stretchr/testify/assert"
)

func TestBasicOperations(t *testing.T) {
	// Setup controller and client
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Test Set operation
	err := client.Set("test-key", "test-value")
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

func TestConcurrentOperations(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Create a channel to signal completion
	done := make(chan bool)

	// Launch multiple goroutines to perform operations concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent-key-%d", id)
			value := fmt.Sprintf("concurrent-value-%d", id)

			// Set value
			err := client.Set(key, value)
			assert.NoError(t, err)

			// Get value
			retrievedValue, err := client.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, value, retrievedValue)

			// Delete value
			err = client.Delete(key)
			assert.NoError(t, err)

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestErrorCases(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Test getting non-existent key
	_, err := client.Get("non-existent-key")
	assert.Error(t, err)

	// Test deleting non-existent key
	err = client.Delete("non-existent-key")
	assert.Error(t, err)

	// Test setting empty key
	err = client.Set("", "value")
	assert.Error(t, err)

	// Test setting empty value
	err = client.Set("key", "")
	assert.Error(t, err)
}

func TestOperationVerification(t *testing.T) {
	ctrl := helpers.SetupTestController(t)
	assert.NotNil(t, ctrl, "Controller should not be nil")
	client := helpers.NewTestClient("http://localhost" + ctrl.GetTestPort())

	// Set a value
	key := "verification-key"
	value := "verification-value"
	err := client.Set(key, value)
	assert.NoError(t, err)

	// Verify the value was set correctly
	retrievedValue, err := client.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Delete the value
	err = client.Delete(key)
	assert.NoError(t, err)

	// Verify the value was deleted
	_, err = client.Get(key)
	assert.Error(t, err)
}
