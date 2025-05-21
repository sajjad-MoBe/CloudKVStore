package shared

import (
    "time"
)

// represents an entry in the Write-Ahead Log
type OperationLogEntry struct {
    Operation    string // "SET", "DELETE"
    Key   string
    Value string // Empty for DELETE
    Timestamp time.Time `json:"timestamp"`
    // Add Timestamp or sequence number later if needed
}