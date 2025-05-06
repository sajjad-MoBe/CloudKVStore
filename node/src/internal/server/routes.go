package server

import (
	"net/http"

)

// @desc Set a key-value pair
// @route PUT /kv/{key}
// @access public
func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	
}

// @desc Get a key-value pair by key
// @route GET /kv/{key}
// @access public
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	
}

// @desc Delete a key-value pair by key
// @route DELETE /kv/{key}
// @access public
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	
}