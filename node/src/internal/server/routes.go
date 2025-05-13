package server

import (
	"net/http"

	"strings"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/shared"

	"encoding/json"
	"log"	

)

// @desc Set a key-value pair
// @route PUT /kv/{key}
// @access public
func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/kv/") // Extract key from URL
	if key == "" {
		http.Error(w, "Missing key in URL path", http.StatusBadRequest)
		return
	}

	var reqBody shared.SetRequestBody
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("Handling SET request: Key=%s, Value=%s", key, reqBody.Value)

	err = s.store.Set(key, reqBody.Value) // Call set on servers' store 

	var resp shared.Response
	if err != nil {
		resp = shared.Response{Status: "ERROR", Error: err.Error()}
		sendJSONResponse(w, http.StatusInternalServerError, resp) // Send error response
	} else {
		resp = shared.Response{Status: "OK"}
		sendJSONResponse(w, http.StatusOK, resp) // Send success response
	}
}

// @desc Get a key-value pair by key
// @route GET /kv/{key}
// @access public
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Missing key in URL path", http.StatusBadRequest)
		return
	}

	log.Printf("Handling GET request: Key=%s", key)

	value, found := s.store.Get(key) // Call get for server's store

	var resp shared.Response
	if !found {
		resp = shared.Response{Status: "NOT_FOUND"}
		sendJSONResponse(w, http.StatusNotFound, resp)
	} else {
		resp = shared.Response{Status: "OK", Value: value}
		sendJSONResponse(w, http.StatusOK, resp)
	}
}

// @desc Delete a key-value pair by key
// @route DELETE /kv/{key}
// @access public
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "Missing key in URL path", http.StatusBadRequest)
		return
	}

	log.Printf("Handling DELETE request: Key=%s", key)

	err := s.store.Delete(key)
	
	var resp shared.Response
	if err != nil {
		//  deleting non-existent key is not handled for now maybe later
		log.Printf("Error during delete (ignoring?): %v", err)
        resp = shared.Response{Status: "OK"} 
		sendJSONResponse(w, http.StatusOK, resp) 
	} else {
		resp = shared.Response{Status: "OK"}
		sendJSONResponse(w, http.StatusOK, resp)
	}
}


func (s *Server) handleWAL(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    log.Printf("Handling WAL request")
    
    entries := s.store.GetWAL()
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status": "OK",
        "wal": entries,
    })
}