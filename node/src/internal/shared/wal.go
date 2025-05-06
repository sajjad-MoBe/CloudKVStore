package shared

// represents an entry in the Write-Ahead Log
type OperationLogEntry struct {
    Op    string // "SET", "DELETE"
    Key   string
    Value string // Empty for DELETE
    // Add Timestamp or sequence number later if needed
}