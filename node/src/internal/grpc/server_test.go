package grpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"cloudkvstore/node/src/internal/storage"
	pb "cloudkvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func init() {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	store := storage.NewMemTable()
	pb.RegisterKeyValueStoreServer(s, NewServer(store))
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func setupTestServer(t *testing.T) (*Server, func()) {
	store := storage.NewMemTable()
	server := NewServer(store)
	return server, func() {
		// Cleanup if needed
	}
}

func TestGet(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test cases
	tests := []struct {
		name    string
		key     string
		value   []byte
		wantErr codes.Code
	}{
		{
			name:    "empty key",
			key:     "",
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "key not found",
			key:     "nonexistent",
			wantErr: codes.NotFound,
		},
		{
			name:  "valid key",
			key:   "test-key",
			value: []byte("test-value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test data
			if tt.value != nil {
				if err := server.store.Set(tt.key, tt.value); err != nil {
					t.Fatalf("Failed to set test data: %v", err)
				}
			}

			// Make request
			req := &pb.GetRequest{Key: tt.key}
			resp, err := server.Get(context.Background(), req)

			// Check error
			if tt.wantErr != codes.OK {
				if err == nil {
					t.Errorf("Expected error %v, got nil", tt.wantErr)
					return
				}
				if st, ok := status.FromError(err); !ok || st.Code() != tt.wantErr {
					t.Errorf("Expected error %v, got %v", tt.wantErr, err)
				}
				return
			}

			// Check response
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if resp.Key != tt.key {
				t.Errorf("Expected key %s, got %s", tt.key, resp.Key)
			}
			if string(resp.Value) != string(tt.value) {
				t.Errorf("Expected value %s, got %s", tt.value, resp.Value)
			}
		})
	}
}

func TestSet(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test cases
	tests := []struct {
		name    string
		key     string
		value   []byte
		wantErr codes.Code
	}{
		{
			name:    "empty key",
			key:     "",
			value:   []byte("value"),
			wantErr: codes.InvalidArgument,
		},
		{
			name:  "valid key-value",
			key:   "test-key",
			value: []byte("test-value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make request
			req := &pb.SetRequest{
				Key:   tt.key,
				Value: tt.value,
			}
			resp, err := server.Set(context.Background(), req)

			// Check error
			if tt.wantErr != codes.OK {
				if err == nil {
					t.Errorf("Expected error %v, got nil", tt.wantErr)
					return
				}
				if st, ok := status.FromError(err); !ok || st.Code() != tt.wantErr {
					t.Errorf("Expected error %v, got %v", tt.wantErr, err)
				}
				return
			}

			// Check response
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if !resp.Success {
				t.Error("Expected success=true")
			}

			// Verify value was set
			value, err := server.store.Get(tt.key)
			if err != nil {
				t.Errorf("Failed to get value: %v", err)
				return
			}
			if string(value) != string(tt.value) {
				t.Errorf("Expected value %s, got %s", tt.value, value)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Test cases
	tests := []struct {
		name    string
		key     string
		setup   bool
		wantErr codes.Code
	}{
		{
			name:    "empty key",
			key:     "",
			wantErr: codes.InvalidArgument,
		},
		{
			name:    "key not found",
			key:     "nonexistent",
			wantErr: codes.NotFound,
		},
		{
			name:  "valid key",
			key:   "test-key",
			setup: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test data
			if tt.setup {
				if err := server.store.Set(tt.key, []byte("test-value")); err != nil {
					t.Fatalf("Failed to set test data: %v", err)
				}
			}

			// Make request
			req := &pb.DeleteRequest{Key: tt.key}
			resp, err := server.Delete(context.Background(), req)

			// Check error
			if tt.wantErr != codes.OK {
				if err == nil {
					t.Errorf("Expected error %v, got nil", tt.wantErr)
					return
				}
				if st, ok := status.FromError(err); !ok || st.Code() != tt.wantErr {
					t.Errorf("Expected error %v, got %v", tt.wantErr, err)
				}
				return
			}

			// Check response
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if !resp.Success {
				t.Error("Expected success=true")
			}

			// Verify value was deleted
			_, err = server.store.Get(tt.key)
			if err != storage.ErrKeyNotFound {
				t.Error("Expected key to be deleted")
			}
		})
	}
}

func TestWatch(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a mock stream
	stream := &mockWatchServer{
		ctx:    context.Background(),
		sendCh: make(chan *pb.WatchResponse, 10),
	}

	// Start watching
	req := &pb.WatchRequest{Key: "test-key"}
	go func() {
		if err := server.Watch(req, stream); err != nil {
			t.Errorf("Watch failed: %v", err)
		}
	}()

	// Set value
	value := []byte("test-value")
	if err := server.store.Set(req.Key, value); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Wait for notification
	select {
	case resp := <-stream.sendCh:
		if resp.Key != req.Key {
			t.Errorf("Expected key %s, got %s", req.Key, resp.Key)
		}
		if string(resp.Value) != string(value) {
			t.Errorf("Expected value %s, got %s", value, resp.Value)
		}
		if resp.Operation != pb.WatchResponse_SET {
			t.Errorf("Expected operation SET, got %v", resp.Operation)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for watch notification")
	}
}

func TestList(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Set up test data
	testData := map[string][]byte{
		"key1":     []byte("value1"),
		"key2":     []byte("value2"),
		"test-key": []byte("test-value"),
	}
	for k, v := range testData {
		if err := server.store.Set(k, v); err != nil {
			t.Fatalf("Failed to set test data: %v", err)
		}
	}

	// Test cases
	tests := []struct {
		name     string
		prefix   string
		expected int
	}{
		{
			name:     "no prefix",
			expected: 3,
		},
		{
			name:     "with prefix",
			prefix:   "key",
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock stream
			stream := &mockListServer{
				ctx:    context.Background(),
				sendCh: make(chan *pb.ListResponse, 10),
			}

			// Start listing
			req := &pb.ListRequest{Prefix: tt.prefix}
			go func() {
				if err := server.List(req, stream); err != nil {
					t.Errorf("List failed: %v", err)
				}
				close(stream.sendCh)
			}()

			// Collect responses
			var responses []*pb.ListResponse
			for resp := range stream.sendCh {
				responses = append(responses, resp)
			}

			// Verify count
			if len(responses) != tt.expected {
				t.Errorf("Expected %d responses, got %d", tt.expected, len(responses))
			}

			// Verify prefix
			for _, resp := range responses {
				if tt.prefix != "" && !strings.HasPrefix(resp.Key, tt.prefix) {
					t.Errorf("Response key %s does not have prefix %s", resp.Key, tt.prefix)
				}
			}
		})
	}
}

// Mock implementations for testing
type mockWatchServer struct {
	pb.KeyValueStore_WatchServer
	ctx    context.Context
	sendCh chan *pb.WatchResponse
}

func (m *mockWatchServer) Context() context.Context {
	return m.ctx
}

func (m *mockWatchServer) Send(resp *pb.WatchResponse) error {
	m.sendCh <- resp
	return nil
}

type mockListServer struct {
	pb.KeyValueStore_ListServer
	ctx    context.Context
	sendCh chan *pb.ListResponse
}

func (m *mockListServer) Context() context.Context {
	return m.ctx
}

func (m *mockListServer) Send(resp *pb.ListResponse) error {
	m.sendCh <- resp
	return nil
}
