package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockStorage struct {
	mock.Mock
}

func (m *mockStorage) Get(key string) (string, error) {
	args := m.Called(key)
	return args.String(0), args.Error(1)
}

func (m *mockStorage) Set(key, value string) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *mockStorage) Delete(key string) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *mockStorage) List(prefix string) ([]string, error) {
	args := m.Called(prefix)
	return args.Get(0).([]string), args.Error(1)
}

func setupTestHandler() (*Handler, *mockStorage) {
	storage := new(mockStorage)
	handler := NewHandler(storage)
	return handler, storage
}

func TestGetValue(t *testing.T) {
	handler, storage := setupTestHandler()

	tests := []struct {
		name           string
		key            string
		mockValue      string
		mockError      error
		expectedStatus int
		expectedBody   map[string]interface{}
	}{
		{
			name:           "success",
			key:            "test-key",
			mockValue:      "test-value",
			mockError:      nil,
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"value": "test-value",
			},
		},
		{
			name:           "not found",
			key:            "non-existent",
			mockValue:      "",
			mockError:      ErrKeyNotFound,
			expectedStatus: http.StatusNotFound,
			expectedBody: map[string]interface{}{
				"error": "key not found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage.On("Get", tt.key).Return(tt.mockValue, tt.mockError)

			req := httptest.NewRequest(http.MethodGet, "/keys/"+tt.key, nil)
			req = mux.SetURLVars(req, map[string]string{"key": tt.key})
			w := httptest.NewRecorder()

			handler.GetValue(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response map[string]interface{}
			err := json.NewDecoder(w.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)
		})
	}
}

func TestSetValue(t *testing.T) {
	handler, storage := setupTestHandler()

	tests := []struct {
		name           string
		key            string
		value          string
		mockError      error
		expectedStatus int
		expectedBody   map[string]interface{}
	}{
		{
			name:           "success",
			key:            "test-key",
			value:          "test-value",
			mockError:      nil,
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"success": true,
			},
		},
		{
			name:           "invalid request",
			key:            "test-key",
			value:          "",
			mockError:      nil,
			expectedStatus: http.StatusBadRequest,
			expectedBody: map[string]interface{}{
				"error": "value is required",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != "" {
				storage.On("Set", tt.key, tt.value).Return(tt.mockError)
			}

			body := map[string]interface{}{
				"value": tt.value,
			}
			jsonBody, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPut, "/keys/"+tt.key, bytes.NewBuffer(jsonBody))
			req = mux.SetURLVars(req, map[string]string{"key": tt.key})
			w := httptest.NewRecorder()

			handler.SetValue(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response map[string]interface{}
			err := json.NewDecoder(w.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)
		})
	}
}

func TestDeleteValue(t *testing.T) {
	handler, storage := setupTestHandler()

	tests := []struct {
		name           string
		key            string
		mockError      error
		expectedStatus int
		expectedBody   map[string]interface{}
	}{
		{
			name:           "success",
			key:            "test-key",
			mockError:      nil,
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"success": true,
			},
		},
		{
			name:           "not found",
			key:            "non-existent",
			mockError:      ErrKeyNotFound,
			expectedStatus: http.StatusNotFound,
			expectedBody: map[string]interface{}{
				"error": "key not found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage.On("Delete", tt.key).Return(tt.mockError)

			req := httptest.NewRequest(http.MethodDelete, "/keys/"+tt.key, nil)
			req = mux.SetURLVars(req, map[string]string{"key": tt.key})
			w := httptest.NewRecorder()

			handler.DeleteValue(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response map[string]interface{}
			err := json.NewDecoder(w.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)
		})
	}
}

func TestListValues(t *testing.T) {
	handler, storage := setupTestHandler()

	tests := []struct {
		name           string
		prefix         string
		mockKeys       []string
		mockError      error
		expectedStatus int
		expectedBody   map[string]interface{}
	}{
		{
			name:           "success",
			prefix:         "test",
			mockKeys:       []string{"test1", "test2"},
			mockError:      nil,
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"keys": []string{"test1", "test2"},
			},
		},
		{
			name:           "empty list",
			prefix:         "",
			mockKeys:       []string{},
			mockError:      nil,
			expectedStatus: http.StatusOK,
			expectedBody: map[string]interface{}{
				"keys": []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage.On("List", tt.prefix).Return(tt.mockKeys, tt.mockError)

			req := httptest.NewRequest(http.MethodGet, "/keys?prefix="+tt.prefix, nil)
			w := httptest.NewRecorder()

			handler.ListValues(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response map[string]interface{}
			err := json.NewDecoder(w.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)
		})
	}
}
