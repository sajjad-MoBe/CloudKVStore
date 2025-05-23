package shared

// represents a server response sent to the client
type Response struct {
	Status string `json:"status"`          // "OK", "ERROR", "NOT_FOUND", ...
	Value  string `json:"value,omitempty"` // only for GET
	Error  string `json:"error,omitempty"` // error message if Status is "ERROR"
}
