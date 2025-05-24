# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/controller ./cmd/controller

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/controller .

# Create directory for controller data
RUN mkdir -p /app/data

# Expose the controller port
EXPOSE 8080

# Set environment variables
ENV CONTROLLER_ADDRESS=:8080

# Run the controller
CMD ["./controller"] 