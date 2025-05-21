package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/errors"
)

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// RecoveryMiddleware is a middleware that recovers panics and writes JSON errors
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				err := errors.RecoverError(rec)
				handleError(w, err)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// handleError writes an error response to the client
func handleError(w http.ResponseWriter, err error) {
	var statusCode int
	var errType string

	switch {
	case errors.IsNotFound(err):
		statusCode = http.StatusNotFound
		errType = "NOT_FOUND"
	case errors.IsInvalidInput(err):
		statusCode = http.StatusBadRequest
		errType = "INVALID_INPUT"
	case errors.IsTimeout(err):
		statusCode = http.StatusGatewayTimeout
		errType = "TIMEOUT"
	default:
		statusCode = http.StatusInternalServerError
		errType = "INTERNAL"
	}

	response := ErrorResponse{}
	response.Error.Type = errType
	response.Error.Message = err.Error()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// LoggingMiddleware logs request details
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a custom response writer to capture the status code
		rw := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(rw, r)

		// Log request details
		logRequest(r, rw.statusCode)
	})
}

// responseWriter is a custom response writer that captures the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// logRequest logs request details
func logRequest(r *http.Request, statusCode int) {
	// TODO: Implement proper logging
	// For now, just print to stdout
	println(r.Method, r.URL.Path, statusCode)
}

// MetricsMiddleware measures request durations (hook into your metrics backend)
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		// TODO: push `duration` to your metrics system
		_ = duration
	})
}
