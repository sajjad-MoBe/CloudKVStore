package api

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// FileAPIKeyStore implements APIKeyStore using the filesystem
type FileAPIKeyStore struct {
	mu       sync.RWMutex
	filePath string
}

// NewFileAPIKeyStore creates a new file-based API key store
func NewFileAPIKeyStore(dataDir string) (*FileAPIKeyStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	return &FileAPIKeyStore{
		filePath: filepath.Join(dataDir, "api_keys.json"),
	}, nil
}

// SaveAPIKey saves an API key and its associated user
func (s *FileAPIKeyStore) SaveAPIKey(key string, user *User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load existing keys
	keys, err := s.loadKeys()
	if err != nil {
		return err
	}

	// Add new key
	keys[key] = user

	// Save to file
	return s.saveKeys(keys)
}

// GetAPIKey retrieves a user by API key
func (s *FileAPIKeyStore) GetAPIKey(key string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys, err := s.loadKeys()
	if err != nil {
		return nil, err
	}

	user, exists := keys[key]
	if !exists {
		return nil, errors.New("API key not found")
	}

	return user, nil
}

// DeleteAPIKey removes an API key
func (s *FileAPIKeyStore) DeleteAPIKey(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Load existing keys
	keys, err := s.loadKeys()
	if err != nil {
		return err
	}

	// Remove key
	delete(keys, key)

	// Save to file
	return s.saveKeys(keys)
}

// loadKeys loads API keys from the file
func (s *FileAPIKeyStore) loadKeys() (map[string]*User, error) {
	keys := make(map[string]*User)

	// Check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return keys, nil
	}

	// Read file
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return nil, err
	}

	// Parse JSON
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, err
	}

	return keys, nil
}

// saveKeys saves API keys to the file
func (s *FileAPIKeyStore) saveKeys(keys map[string]*User) error {
	// Marshal to JSON
	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		return err
	}

	// Write to file
	return os.WriteFile(s.filePath, data, 0644)
}
