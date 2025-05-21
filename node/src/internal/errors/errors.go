package errors

import (
	"fmt"
	"runtime"
)

// ErrorType represents the type of error
type ErrorType string

const (
	// ErrorTypeNotFound indicates the requested resource was not found
	ErrorTypeNotFound ErrorType = "NOT_FOUND"
	// ErrorTypeInvalidInput indicates invalid input parameters
	ErrorTypeInvalidInput ErrorType = "INVALID_INPUT"
	// ErrorTypeInternal indicates an internal server error
	ErrorTypeInternal ErrorType = "INTERNAL"
	// ErrorTypeStorage indicates a storage-related error
	ErrorTypeStorage ErrorType = "STORAGE"
	// ErrorTypeTimeout indicates an operation timed out
	ErrorTypeTimeout ErrorType = "TIMEOUT"
)

// KVError represents a custom error with additional context
type KVError struct {
	Type    ErrorType
	Message string
	Err     error
	Stack   string
}

// Error implements the error interface
func (e *KVError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s (%s)", e.Type, e.Message, e.Err.Error())
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

// Unwrap returns the wrapped error
func (e *KVError) Unwrap() error {
	return e.Err
}

// New creates a new KVError
func New(errType ErrorType, message string, err error) *KVError {
	// Capture stack trace
	_, file, line, _ := runtime.Caller(1)
	stack := fmt.Sprintf("%s:%d", file, line)

	return &KVError{
		Type:    errType,
		Message: message,
		Err:     err,
		Stack:   stack,
	}
}

// IsNotFound checks if the error is a not found error
func IsNotFound(err error) bool {
	if kvErr, ok := err.(*KVError); ok {
		return kvErr.Type == ErrorTypeNotFound
	}
	return false
}

// IsInvalidInput checks if the error is an invalid input error
func IsInvalidInput(err error) bool {
	if kvErr, ok := err.(*KVError); ok {
		return kvErr.Type == ErrorTypeInvalidInput
	}
	return false
}

// IsInternal checks if the error is an internal error
func IsInternal(err error) bool {
	if kvErr, ok := err.(*KVError); ok {
		return kvErr.Type == ErrorTypeInternal
	}
	return false
}

// IsStorage checks if the error is a storage error
func IsStorage(err error) bool {
	if kvErr, ok := err.(*KVError); ok {
		return kvErr.Type == ErrorTypeStorage
	}
	return false
}

// IsTimeout checks if the error is a timeout error
func IsTimeout(err error) bool {
	if kvErr, ok := err.(*KVError); ok {
		return kvErr.Type == ErrorTypeTimeout
	}
	return false
}

// RecoverError recovers from a panic and converts it to a KVError
func RecoverError(r interface{}) error {
	if r == nil {
		return nil
	}

	var err error
	switch v := r.(type) {
	case error:
		err = v
	case string:
		err = fmt.Errorf("%s", v)
	default:
		err = fmt.Errorf("%v", v)
	}

	return New(ErrorTypeInternal, "recovered from panic", err)
}
