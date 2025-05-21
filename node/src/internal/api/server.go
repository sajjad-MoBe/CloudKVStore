package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"

	"github.com/gorilla/mux"
)

// Server represents the HTTP API server
type Server struct {
	router *mux.Router
	store  *storage.MemTable
}

// NewServer creates a new API server instance
func NewServer(store *storage.MemTable) *Server {
	s := &Server{
		router: mux.NewRouter(),
		store:  store,
	}
	s.setupRoutes()
	return s
}

// setupRoutes configures all API routes
func (s *Server) setupRoutes() {
	// Key-value operations
	s.router.HandleFunc("/kv/{key}", s.handleGet).Methods("GET")
	s.router.HandleFunc("/kv/{key}", s.handlePut).Methods("PUT")
	s.router.HandleFunc("/kv/{key}", s.handleDelete).Methods("DELETE")

	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
}

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	return http.ListenAndServe(addr, s.router)
}

// handleGet handles GET /kv/{key} requests
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := s.store.Get(key)
	if err != nil {
		var errKeyNotFound *storage.ErrKeyNotFound
		if errors.As(err, &errKeyNotFound) {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": string(value),
	})
	if err != nil {
		return
	}
}

// handlePut handles PUT /kv/{key} requests
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var req struct {
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		http.Error(w, "Value cannot be empty", http.StatusBadRequest)
		return
	}

	if err := s.store.Set(key, []byte(req.Value)); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDelete handles DELETE /kv/{key} requests
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if err := s.store.Delete(key); err != nil {
		var errKeyNotFound *storage.ErrKeyNotFound
		if errors.As(err, &errKeyNotFound) {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleHealth handles GET /health requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
	if err != nil {
		return
	}
}
