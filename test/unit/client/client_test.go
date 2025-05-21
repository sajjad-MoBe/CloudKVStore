package client_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/client"
	"github.com/stretchr/testify/assert"
)

func TestClientRetries(t *testing.T) {
	var attempts int32
	// Mock server that fails 3 times before succeeding
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := client.NewClient(ts.URL, client.RetryConfig{MaxRetries: 5})
	err := c.Set("key", []byte("value"))
	assert.NoError(t, err) // Should succeed after retries
}

func TestBatchClient(t *testing.T) {
	batch := client.NewBatchClient(client.DefaultRetryConfig(), 10)
	batch.Set("k1", []byte("v1"))
	batch.Set("k2", []byte("v2"))
	result := batch.Wait()
	assert.Equal(t, 2, len(result.Successful))
}
