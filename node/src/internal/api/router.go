package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// Router creates and configures the HTTP router
func Router(handler *Handler, authManager *AuthManager) http.Handler {
	router := mux.NewRouter()

	// Apply global middleware
	router.Use(
		SecurityHeaders,
		CORSMiddleware,
		LoggingMiddleware,
		RecoveryMiddleware,
		MetricsMiddleware,
	)

	// API routes
	api := router.PathPrefix("/api/v1").Subrouter()

	// Public endpoints (no auth required)
	api.HandleFunc("/health", handler.HealthCheck).Methods(http.MethodGet)

	// Protected endpoints
	protected := api.PathPrefix("").Subrouter()
	protected.Use(authManager.AuthMiddleware(RoleUser))

	// Read-only endpoints
	readOnly := protected.PathPrefix("").Subrouter()
	readOnly.Use(authManager.AuthMiddleware(RoleRead))
	readOnly.HandleFunc("/keys", handler.ListValues).Methods(http.MethodGet)
	readOnly.HandleFunc("/keys/{key}", handler.GetValue).Methods(http.MethodGet)

	// Write endpoints
	write := protected.PathPrefix("").Subrouter()
	write.Use(authManager.AuthMiddleware(RoleWrite))
	write.HandleFunc("/keys/{key}", handler.SetValue).Methods(http.MethodPut)
	write.HandleFunc("/keys/{key}", handler.DeleteValue).Methods(http.MethodDelete)

	// Admin endpoints
	admin := api.PathPrefix("/admin").Subrouter()
	admin.Use(authManager.AuthMiddleware(RoleAdmin))
	admin.HandleFunc("/users", handler.CreateUser).Methods(http.MethodPost)
	admin.HandleFunc("/users/{id}", handler.DeleteUser).Methods(http.MethodDelete)
	admin.HandleFunc("/metrics", handler.GetMetrics).Methods(http.MethodGet)

	// Swagger documentation
	router.PathPrefix("/swagger/").Handler(http.StripPrefix("/swagger/", http.FileServer(http.Dir("swagger"))))

	return router
}

// LoggingMiddleware logs request details
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrw := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(wrw, r)
		duration := time.Since(start)
		logRequest(r, wrw.statusCode, duration)
	})
}

// RecoveryMiddleware recovers from panics
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				handleError(w, err, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// MetricsMiddleware tracks request metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		// TODO: Add metrics collection
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// logRequest logs request details
func logRequest(r *http.Request, statusCode int, duration time.Duration) {
	// TODO: Implement proper logging
}

// handleError writes an error response
func handleError(w http.ResponseWriter, err interface{}, status int) {
	response := map[string]interface{}{
		"error": err,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}
