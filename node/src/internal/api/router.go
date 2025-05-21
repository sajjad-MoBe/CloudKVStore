package api

import (
	"github.com/gorilla/mux"
	"net/http"
)

// Router creates and configures the HTTP router
func Router(handler *Handler, authManager *AuthManager) http.Handler {
	router := mux.NewRouter()

	// Apply global middleware
	router.Use(
		SecurityHeaders,
		CORSMiddleware,
		LoggingMiddleware,  // from middleware.go
		RecoveryMiddleware, // was ErrorHandler before
		MetricsMiddleware,  // now defined in middleware.go
	)

	// API routes
	api := router.PathPrefix("/api/v1").Subrouter()

	// Public endpoints (no auth required)
	api.HandleFunc("/health", handler.HealthCheckHandler).Methods(http.MethodGet)

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
	admin.HandleFunc("/metrics", handler.MetricsHandler).Methods(http.MethodGet)

	// Swagger documentation
	router.PathPrefix("/swagger/").Handler(http.StripPrefix("/swagger/", http.FileServer(http.Dir("swagger"))))

	return router
}
