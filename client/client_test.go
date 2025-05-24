package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTestServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/kv/test-key":
			switch r.Method {
			case http.MethodGet:
				json.NewEncoder(w).Encode(Response{
					Success: true,
					Value:   "test-value",
				})
			case http.MethodPut:
				var reqBody map[string]string
				if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
					http.Error(w, "Invalid request body", http.StatusBadRequest)
					return
				}
				json.NewEncoder(w).Encode(Response{Success: true})
			case http.MethodDelete:
				json.NewEncoder(w).Encode(Response{Success: true})
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		case "/api/v1/cluster/status":
			json.NewEncoder(w).Encode(Response{
				Success: true,
				Data: map[string]interface{}{
					"nodes":  []string{"node1", "node2", "node3"},
					"status": "healthy",
				},
			})
		default:
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
}

func TestClientSet(t *testing.T) {
	server := setupTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{
			name:    "successful set",
			key:     "test-key",
			value:   "test-value",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Set(tt.key, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientGet(t *testing.T) {
	server := setupTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{
			name:    "successful get",
			key:     "test-key",
			want:    "test-value",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Client.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientDelete(t *testing.T) {
	server := setupTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "successful delete",
			key:     "test-key",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Delete(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientGetClusterStatus(t *testing.T) {
	server := setupTestServer(t)
	defer server.Close()

	client := NewClient(server.URL)

	tests := []struct {
		name    string
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "successful status",
			want: map[string]interface{}{
				"nodes":  []string{"node1", "node2", "node3"},
				"status": "healthy",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetClusterStatus()
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.GetClusterStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got["status"] != tt.want["status"] {
				t.Errorf("Client.GetClusterStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientTimeout(t *testing.T) {
	// Create a server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // Simulate delay
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with short timeout
	client := &Client{
		nodeAddr: server.URL,
		client: &http.Client{
			Timeout: 1 * time.Second,
		},
	}

	// Test should fail due to timeout
	_, err := client.Get("test-key")
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestClientInvalidResponse(t *testing.T) {
	// Create a server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// Test should fail due to invalid JSON
	_, err := client.Get("test-key")
	if err == nil {
		t.Error("Expected JSON decode error, got nil")
	}
}
