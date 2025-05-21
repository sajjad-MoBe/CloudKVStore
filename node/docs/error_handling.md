# Error Handling

This document describes the error handling mechanisms in CloudKVStore.

## Error Types

The system defines several error types to categorize different kinds of errors:

- `NOT_FOUND`: The requested resource was not found
- `INVALID_INPUT`: Invalid input parameters
- `INTERNAL`: Internal server error
- `STORAGE`: Storage-related error
- `TIMEOUT`: Operation timed out

## Error Structure

Errors in CloudKVStore follow a consistent structure:

```json
{
  "error": {
    "type": "ERROR_TYPE",
    "message": "Error description"
  }
}
```

## HTTP API Error Handling

### Status Codes

The HTTP API uses standard HTTP status codes:

- `400 Bad Request`: Invalid input parameters
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server error
- `504 Gateway Timeout`: Operation timed out

### Error Recovery

The HTTP API includes middleware for error recovery:

1. Panic Recovery: Catches and handles panics
2. Error Conversion: Converts internal errors to HTTP responses
3. Logging: Logs errors with stack traces

## gRPC Error Handling

### Status Codes

The gRPC API uses gRPC status codes:

- `INVALID_ARGUMENT`: Invalid input parameters
- `NOT_FOUND`: Resource not found
- `INTERNAL`: Server error
- `DEADLINE_EXCEEDED`: Operation timed out

### Error Recovery

The gRPC API includes interceptors for error recovery:

1. Unary Interceptor: Handles errors in unary RPCs
2. Stream Interceptor: Handles errors in streaming RPCs
3. Error Conversion: Converts internal errors to gRPC status errors

## Error Handling Best Practices

1. **Always Check Errors**
   ```go
   if err != nil {
       return errors.New(errors.ErrorTypeInternal, "operation failed", err)
   }
   ```

2. **Use Custom Error Types**
   ```go
   if err := validateInput(input); err != nil {
       return errors.New(errors.ErrorTypeInvalidInput, "invalid input", err)
   }
   ```

3. **Include Context**
   ```go
   return errors.New(errors.ErrorTypeStorage, "failed to write to storage", err)
   ```

4. **Handle Panics**
   ```go
   defer func() {
       if r := recover(); r != nil {
           err := errors.RecoverError(r)
           // Handle error
       }
   }()
   ```

5. **Log Errors**
   ```go
   if err != nil {
       log.Printf("Error: %v\nStack: %s", err, err.(*errors.KVError).Stack)
   }
   ```

## Error Recovery Mechanisms

1. **Panic Recovery**
   - Catches unexpected panics
   - Converts panics to errors
   - Includes stack traces

2. **Timeout Handling**
   - Sets appropriate timeouts
   - Returns timeout errors
   - Cleans up resources

3. **Resource Cleanup**
   - Ensures resources are released
   - Handles partial failures
   - Maintains consistency

## Error Handling in Tests

1. **Test Error Cases**
   ```go
   func TestErrorHandling(t *testing.T) {
       err := someOperation()
       if !errors.IsNotFound(err) {
           t.Errorf("Expected not found error, got %v", err)
       }
   }
   ```

2. **Test Panic Recovery**
   ```go
   func TestPanicRecovery(t *testing.T) {
       defer func() {
           if r := recover(); r != nil {
               t.Error("Unexpected panic")
           }
       }()
       // Test code
   }
   ```

3. **Test Error Types**
   ```go
   func TestErrorTypes(t *testing.T) {
       testCases := []struct {
           err      error
           expected errors.ErrorType
       }{
           // Test cases
       }
       // Run tests
   }
   ``` 