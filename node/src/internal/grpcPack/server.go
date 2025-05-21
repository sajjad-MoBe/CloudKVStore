package grpcPack

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	kvErr "github.com/sajjad-MoBe/CloudKVStore/node/src/internal/errors"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"
	pb "github.com/sajjad-MoBe/CloudKVStore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the KeyValueStore gRPC service
type Server struct {
	pb.UnimplementedKeyValueStoreServer
	store   *storage.MemTable
	mu      sync.RWMutex
	metrics *ServerMetrics
	// Map of key to channel for watch operations
	watchers map[string][]chan *pb.WatchResponse
}

// ServerMetrics tracks gRPC server metrics
type ServerMetrics struct {
	mu sync.RWMutex
	// Operation counts
	GetCount    int64
	SetCount    int64
	DeleteCount int64
	WatchCount  int64
	ListCount   int64
	// Error counts
	ErrorCount int64
	// Latency metrics
	GetLatency    time.Duration
	SetLatency    time.Duration
	DeleteLatency time.Duration
}

// NewServer creates a new gRPC server instance
func NewServer(store *storage.MemTable) *Server {
	return &Server{
		store:    store,
		watchers: make(map[string][]chan *pb.WatchResponse),
		metrics:  &ServerMetrics{},
	}
}

// validateKey validates a key
func validateKey(key string) error {
	if key == "" {
		return kvErr.New(kvErr.ErrorTypeInvalidInput, "key cannot be empty", nil)
	}
	if len(key) > 1024 {
		return kvErr.New(kvErr.ErrorTypeInvalidInput, "key too long", nil)
	}
	return nil
}

// Get implements the Get RPC method
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	defer func() {
		s.metrics.mu.Lock()
		s.metrics.GetCount++
		s.metrics.GetLatency += time.Since(start)
		s.metrics.mu.Unlock()
	}()

	// Validate request
	if err := validateKey(req.Key); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Check context
	if ctx.Err() != nil {
		return nil, status.Error(codes.Canceled, "request canceled")
	}

	// Get value
	value, err := s.store.Get(req.Key)
	if err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		var errKeyNotFound *storage.ErrKeyNotFound
		if errors.As(err, &errKeyNotFound) {
			return nil, status.Error(codes.NotFound, "key not found")
		}

		return nil, status.Error(codes.Internal, "internal error")
	}

	return &pb.GetResponse{
		Key:   req.Key,
		Value: value,
	}, nil
}

// Set implements the Set RPC method
func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	start := time.Now()
	defer func() {
		s.metrics.mu.Lock()
		s.metrics.SetCount++
		s.metrics.SetLatency += time.Since(start)
		s.metrics.mu.Unlock()
	}()

	// Validate request
	if err := validateKey(req.Key); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Check context
	if ctx.Err() != nil {
		return nil, status.Error(codes.Canceled, "request canceled")
	}

	// Set value
	if err := s.store.Set(req.Key, req.Value); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		return nil, status.Error(codes.Internal, "internal error")
	}

	// Notify watchers
	s.notifyWatchers(req.Key, req.Value, pb.WatchResponse_SET)

	return &pb.SetResponse{Success: true}, nil
}

// Delete implements the Delete RPC method
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer func() {
		s.metrics.mu.Lock()
		s.metrics.DeleteCount++
		s.metrics.DeleteLatency += time.Since(start)
		s.metrics.mu.Unlock()
	}()

	// Validate request
	if err := validateKey(req.Key); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Check context
	if ctx.Err() != nil {
		return nil, status.Error(codes.Canceled, "request canceled")
	}

	// Delete value
	if err := s.store.Delete(req.Key); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		var errKeyNotFound *storage.ErrKeyNotFound
		if errors.As(err, &errKeyNotFound) {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Error(codes.Internal, "internal error")
	}

	// Notify watchers
	s.notifyWatchers(req.Key, nil, pb.WatchResponse_DELETE)

	return &pb.DeleteResponse{Success: true}, nil
}

// Watch implements the Watch RPC method
func (s *Server) Watch(req *pb.WatchRequest, stream pb.KeyValueStore_WatchServer) error {
	s.metrics.mu.Lock()
	s.metrics.WatchCount++
	s.metrics.mu.Unlock()

	// Validate request
	if err := validateKey(req.Key); err != nil {
		s.metrics.mu.Lock()
		s.metrics.ErrorCount++
		s.metrics.mu.Unlock()
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// Create channel for this watcher
	ch := make(chan *pb.WatchResponse, 100) // Increased buffer size
	defer close(ch)

	// Register watcher
	s.mu.Lock()
	s.watchers[req.Key] = append(s.watchers[req.Key], ch)
	s.mu.Unlock()

	// Cleanup on exit
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		watchers := s.watchers[req.Key]
		for i, w := range watchers {
			if w == ch {
				s.watchers[req.Key] = append(watchers[:i], watchers[i+1:]...)
				break
			}
		}
	}()

	// Stream updates
	for {
		select {
		case <-stream.Context().Done():
			return status.Error(codes.Canceled, "client disconnected")
		case response := <-ch:
			if err := stream.Send(response); err != nil {
				s.metrics.mu.Lock()
				s.metrics.ErrorCount++
				s.metrics.mu.Unlock()
				return status.Error(codes.Internal, "failed to send response")
			}
		}
	}
}

// List implements the List RPC method
func (s *Server) List(req *pb.ListRequest, stream pb.KeyValueStore_ListServer) error {
	s.metrics.mu.Lock()
	s.metrics.ListCount++
	s.metrics.mu.Unlock()

	// Check context
	if stream.Context().Err() != nil {
		return status.Error(codes.Canceled, "request canceled")
	}

	// Get all key-value pairs
	pairs := s.store.GetAll()

	// Stream each pair
	for key, value := range pairs {
		// Skip if prefix doesn't match
		if req.Prefix != "" && !strings.HasPrefix(key, req.Prefix) {
			continue
		}

		// Check context before sending
		if stream.Context().Err() != nil {
			return status.Error(codes.Canceled, "request canceled")
		}

		response := &pb.ListResponse{
			Key:   key,
			Value: value,
		}

		if err := stream.Send(response); err != nil {
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.mu.Unlock()
			return status.Error(codes.Internal, "failed to send response")
		}
	}

	return nil
}

// GetMetrics returns the current server metrics
func (s *Server) GetMetrics() *ServerMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return s.metrics
}

// notifyWatchers sends notifications to all watchers for a key
func (s *Server) notifyWatchers(key string, value []byte, op pb.WatchResponse_Operation) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	response := &pb.WatchResponse{
		Key:       key,
		Value:     value,
		Operation: op,
	}

	for _, ch := range s.watchers[key] {
		select {
		case ch <- response:
		default:
			// Skip if channel is full
			s.metrics.mu.Lock()
			s.metrics.ErrorCount++
			s.metrics.mu.Unlock()
		}
	}
}
