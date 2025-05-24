package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func setupLoadTestServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Simulate some processing time
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

		switch r.URL.Path {
		case "/api/v1/kv/":
			switch r.Method {
			case http.MethodGet:
				json.NewEncoder(w).Encode(Response{
					Success: true,
					Value:   "test-value",
				})
			case http.MethodPut:
				json.NewEncoder(w).Encode(Response{Success: true})
			case http.MethodDelete:
				json.NewEncoder(w).Encode(Response{Success: true})
			}
		case "/api/v1/cluster/status":
			json.NewEncoder(w).Encode(Response{
				Success: true,
				Data: map[string]interface{}{
					"nodes":  []string{"node1", "node2", "node3"},
					"status": "healthy",
				},
			})
		}
	}))
}

func runLoadTest(t *testing.T, client *Client, numOperations int, numConcurrent int, operation func() error) time.Duration {
	var wg sync.WaitGroup
	start := time.Now()

	// Create a channel to limit concurrent operations
	semaphore := make(chan struct{}, numConcurrent)

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			if err := operation(); err != nil {
				t.Errorf("Operation failed: %v", err)
			}
		}()
	}

	wg.Wait()
	return time.Since(start)
}

func TestLoadSet(t *testing.T) {
	server := setupLoadTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	testCases := []struct {
		name          string
		numOperations int
		numConcurrent int
		expectedRPS   float64
	}{
		{
			name:          "Low Load",
			numOperations: 100,
			numConcurrent: 10,
			expectedRPS:   50,
		},
		{
			name:          "Medium Load",
			numOperations: 500,
			numConcurrent: 50,
			expectedRPS:   100,
		},
		{
			name:          "High Load",
			numOperations: 1000,
			numConcurrent: 100,
			expectedRPS:   200,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration := runLoadTest(t, client, tc.numOperations, tc.numConcurrent, func() error {
				key := fmt.Sprintf("key-%d", rand.Intn(1000))
				value := fmt.Sprintf("value-%d", rand.Intn(1000))
				return client.Set(key, value)
			})

			rps := float64(tc.numOperations) / duration.Seconds()
			t.Logf("Operations: %d, Concurrent: %d, Duration: %v, RPS: %.2f",
				tc.numOperations, tc.numConcurrent, duration, rps)

			if rps < tc.expectedRPS {
				t.Errorf("Performance below expected: got %.2f RPS, want at least %.2f RPS",
					rps, tc.expectedRPS)
			}
		})
	}
}

func TestLoadGet(t *testing.T) {
	server := setupLoadTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	// First, set some values
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := client.Set(key, value); err != nil {
			t.Fatalf("Failed to set initial values: %v", err)
		}
	}

	testCases := []struct {
		name          string
		numOperations int
		numConcurrent int
		expectedRPS   float64
	}{
		{
			name:          "Low Load",
			numOperations: 100,
			numConcurrent: 10,
			expectedRPS:   50,
		},
		{
			name:          "Medium Load",
			numOperations: 500,
			numConcurrent: 50,
			expectedRPS:   100,
		},
		{
			name:          "High Load",
			numOperations: 1000,
			numConcurrent: 100,
			expectedRPS:   200,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration := runLoadTest(t, client, tc.numOperations, tc.numConcurrent, func() error {
				key := fmt.Sprintf("key-%d", rand.Intn(1000))
				_, err := client.Get(key)
				return err
			})

			rps := float64(tc.numOperations) / duration.Seconds()
			t.Logf("Operations: %d, Concurrent: %d, Duration: %v, RPS: %.2f",
				tc.numOperations, tc.numConcurrent, duration, rps)

			if rps < tc.expectedRPS {
				t.Errorf("Performance below expected: got %.2f RPS, want at least %.2f RPS",
					rps, tc.expectedRPS)
			}
		})
	}
}

func TestLoadMixed(t *testing.T) {
	server := setupLoadTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	testCases := []struct {
		name          string
		numOperations int
		numConcurrent int
		expectedRPS   float64
	}{
		{
			name:          "Low Load",
			numOperations: 300,
			numConcurrent: 30,
			expectedRPS:   50,
		},
		{
			name:          "Medium Load",
			numOperations: 1500,
			numConcurrent: 150,
			expectedRPS:   100,
		},
		{
			name:          "High Load",
			numOperations: 3000,
			numConcurrent: 300,
			expectedRPS:   200,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration := runLoadTest(t, client, tc.numOperations, tc.numConcurrent, func() error {
				// Randomly choose operation type
				switch rand.Intn(3) {
				case 0:
					key := fmt.Sprintf("key-%d", rand.Intn(1000))
					value := fmt.Sprintf("value-%d", rand.Intn(1000))
					return client.Set(key, value)
				case 1:
					key := fmt.Sprintf("key-%d", rand.Intn(1000))
					_, err := client.Get(key)
					return err
				case 2:
					key := fmt.Sprintf("key-%d", rand.Intn(1000))
					return client.Delete(key)
				}
				return nil
			})

			rps := float64(tc.numOperations) / duration.Seconds()
			t.Logf("Operations: %d, Concurrent: %d, Duration: %v, RPS: %.2f",
				tc.numOperations, tc.numConcurrent, duration, rps)

			if rps < tc.expectedRPS {
				t.Errorf("Performance below expected: got %.2f RPS, want at least %.2f RPS",
					rps, tc.expectedRPS)
			}
		})
	}
}

func TestLoadWithFailures(t *testing.T) {
	server := setupLoadTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	// Test with simulated failures
	numOperations := 1000
	failureRate := 0.1 // 10% failure rate

	var failures int
	var wg sync.WaitGroup
	start := time.Now()

	// Create a channel to limit concurrent operations
	semaphore := make(chan struct{}, 100) // Limit to 100 concurrent operations

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore

		go func() {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			// Simulate random failures
			if rand.Float64() < failureRate {
				failures++
				return
			}

			key := fmt.Sprintf("key-%d", rand.Intn(1000))
			value := fmt.Sprintf("value-%d", rand.Intn(1000))
			if err := client.Set(key, value); err != nil {
				t.Errorf("Operation failed: %v", err)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	successRate := float64(numOperations-failures) / float64(numOperations)
	rps := float64(numOperations) / duration.Seconds()

	t.Logf("Operations: %d, Failures: %d, Success Rate: %.2f%%, RPS: %.2f",
		numOperations, failures, successRate*100, rps)

	if successRate < (1 - failureRate - 0.05) { // Allow 5% margin
		t.Errorf("Success rate too low: got %.2f%%, want at least %.2f%%",
			successRate*100, (1-failureRate)*100)
	}
}
