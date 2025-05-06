package shared

// a client request received by the server
type Request struct {
    Operation    string // "SET", "GET", "DELETE"
    Key   string
    Value string // Empty for GET/DELETE
}