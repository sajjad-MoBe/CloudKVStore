package server

import (
	"context"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"
	"net/http"

	"fmt"
	"log"
)

type Server struct {
	store   *storage.SinglePartitionStore
	address string
}

func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

func NewServer(store *storage.SinglePartitionStore, address string) *Server {
	return &Server{
		store:   store,
		address: address,
	}
}

func (s *Server) Start() error {
	// Create a ServeMux (router)
	mux := http.NewServeMux()

	// We'll route based on path prefix /kv/ and handle methods inside
	// TODO: Implement  handle methods inside
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleGet(w, r)
		case http.MethodPut:
			s.handleSet(w, r)
		case http.MethodDelete:
			s.handleDelete(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Node HTTP server starting on %s", s.address)

	// Start the HTTP server
	err := http.ListenAndServe(s.address, mux)
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	return nil
}
