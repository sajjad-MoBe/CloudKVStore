package grpcPack

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/errors"
	"net/http"
	"runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryErrorInterceptor is a gRPC interceptor that handles errors
func UnaryErrorInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			err := errors.RecoverError(r)
			// Convert to gRPC status error
			_ = status.Error(codes.Internal, err.Error())
		}
	}()

	resp, err := handler(ctx, req)
	if err != nil {
		return nil, convertError(err)
	}
	return resp, nil
}

// StreamErrorInterceptor is a gRPC interceptor that handles errors in streams
func StreamErrorInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	defer func() {
		if r := recover(); r != nil {
			err := errors.RecoverError(r)
			// Convert to gRPC status error
			_ = status.Error(codes.Internal, err.Error())
		}
	}()

	return handler(srv, ss)
}

// convertError converts a KVError to a gRPC status error
func convertError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.IsNotFound(err):
		return status.Error(codes.NotFound, err.Error())
	case errors.IsInvalidInput(err):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.IsTimeout(err):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.IsStorage(err):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// RecoveryMiddleware is a middleware that recovers from panics
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				// Get stack trace
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				stack := string(buf[:n])

				// Create error
				err := errors.New(errors.ErrorTypeInternal, "recovered from panic", fmt.Errorf("%v\n%s", r, stack))
				handleError(w, err)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// handleError writes an error response
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

	response := struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}{
		Error: struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}{
			Type:    errType,
			Message: err.Error(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
