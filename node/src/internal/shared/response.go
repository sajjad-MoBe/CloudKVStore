package shared

// represents a server response sent to the client
type Response struct {
    Status string // "OK", "ERROR", "NOT_FOUND", ...
    Value  string // only for GET
    Error  string // error message if Status is "ERROR"
}
