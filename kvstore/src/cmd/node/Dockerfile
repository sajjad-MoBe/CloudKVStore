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
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/node ./cmd/node

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/node .

# Create directory for WAL files
RUN mkdir -p /app/data/wal

# Expose the node port
EXPOSE 8080

# Set environment variables
ENV NODE_ID=node1
ENV NODE_ADDRESS=:8080
ENV CONTROLLER_URL=http://controller:8080

# Run the node
CMD ["./node"] 