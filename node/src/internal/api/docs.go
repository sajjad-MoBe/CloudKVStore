package api

// @title CloudKVStore API
// @version 1.0
// @description A simple key-value store API
// @host localhost:8080
// @BasePath /

// @tag.name Key-Value Operations
// @tag.description Operations for managing key-value pairs

// @summary Get value by key
// @description Retrieves the value associated with the given key
// @tags Key-Value Operations
// @accept json
// @produce json
// @param key path string true "Key to retrieve"
// @success 200 {object} map[string]string "Key-value pair"
// @failure 404 {string} string "Key not found"
// @failure 500 {string} string "Internal server error"
// @router /kv/{key} [get]

// @summary Set value for key
// @description Sets the value for the given key
// @tags Key-Value Operations
// @accept json
// @produce json
// @param key path string true "Key to set"
// @param value body object true "Value to set"
// @success 200 {string} string "Success"
// @failure 400 {string} string "Invalid request"
// @failure 500 {string} string "Internal server error"
// @router /kv/{key} [put]

// @summary Delete key
// @description Deletes the key-value pair
// @tags Key-Value Operations
// @accept json
// @produce json
// @param key path string true "Key to delete"
// @success 200 {string} string "Success"
// @failure 404 {string} string "Key not found"
// @failure 500 {string} string "Internal server error"
// @router /kv/{key} [delete]

// @summary Health check
// @description Check if the service is healthy
// @tags Health
// @accept json
// @produce json
// @success 200 {object} map[string]string "Service status"
// @router /health [get]
