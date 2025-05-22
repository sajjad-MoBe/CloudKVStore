package shared

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"sync"
	"time"
)

// Role represents a user role
type Role string

const (
	RoleAdmin Role = "admin"
	RoleUser  Role = "user"
	RoleRead  Role = "read"
	RoleWrite Role = "write"
)

// User represents an authenticated user
type User struct {
	ID        string
	Roles     []Role
	APIKey    string
	CreatedAt time.Time
	LastUsed  time.Time
}

// AuthManager manages authentication and authorization
type AuthManager struct {
	mu      sync.RWMutex
	users   map[string]*User
	apiKeys map[string]*User
	store   APIKeyStore
}

// APIKeyStore defines the interface for storing API keys
type APIKeyStore interface {
	SaveAPIKey(key string, user *User) error
	GetAPIKey(key string) (*User, error)
	DeleteAPIKey(key string) error
}

// NewAuthManager creates a new authentication manager
func NewAuthManager(store APIKeyStore) *AuthManager {
	return &AuthManager{
		users:   make(map[string]*User),
		apiKeys: make(map[string]*User),
		store:   store,
	}
}

// GenerateAPIKey generates a new API key
func (am *AuthManager) GenerateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// CreateUser creates a new user with the specified roles
func (am *AuthManager) CreateUser(id string, roles []Role) (*User, error) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.users[id]; exists {
		return nil, errors.New("user already exists")
	}

	apiKey, err := am.GenerateAPIKey()
	if err != nil {
		return nil, err
	}

	user := &User{
		ID:        id,
		Roles:     roles,
		APIKey:    apiKey,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	am.users[id] = user
	am.apiKeys[apiKey] = user

	if err := am.store.SaveAPIKey(apiKey, user); err != nil {
		delete(am.users, id)
		delete(am.apiKeys, apiKey)
		return nil, err
	}

	return user, nil
}

// Authenticate authenticates a request using the API key
func (am *AuthManager) Authenticate(r *http.Request) (*User, error) {
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		return nil, errors.New("missing API key")
	}

	am.mu.RLock()
	user, exists := am.apiKeys[apiKey]
	am.mu.RUnlock()

	if !exists {
		// Try to load from store
		var err error
		user, err = am.store.GetAPIKey(apiKey)
		if err != nil {
			return nil, errors.New("invalid API key")
		}
		am.mu.Lock()
		am.apiKeys[apiKey] = user
		am.mu.Unlock()
	}

	user.LastUsed = time.Now()
	return user, nil
}

// Authorize checks if a user has the required role
func (am *AuthManager) Authorize(user *User, requiredRole Role) bool {
	for _, role := range user.Roles {
		if role == requiredRole || role == RoleAdmin {
			return true
		}
	}
	return false
}

// AuthMiddleware is middleware for authentication and authorization
func (am *AuthManager) AuthMiddleware(requiredRole Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, err := am.Authenticate(r)
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			if !am.Authorize(user, requiredRole) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), "user", user)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// SecurityHeaders adds security headers to responses
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}

// CORSMiddleware handles CORS
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
