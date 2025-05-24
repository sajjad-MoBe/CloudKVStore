# Use the official Go image as the base image
FROM golang:1.21

# Set the working directory
WORKDIR /app

# Copy go mod and sum files
COPY src/go.mod src/go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY src/ .

# Build the application
RUN go build -o kvstore

# Expose the port
EXPOSE 8081 8082 8083

# Command to run the application
CMD ["./kvstore"]
