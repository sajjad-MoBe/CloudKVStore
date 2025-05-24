package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	DefaultNodeAddr = "http://localhost:8081"
	APIPrefix       = "/api/v1"
)

type Client struct {
	nodeAddr string
	client   *http.Client
}

type Response struct {
	Status string      `json:"status"`          // "OK", "ERROR", "NOT_FOUND", ...
	Value  string      `json:"value,omitempty"` // only for GET
	Error  string      `json:"error,omitempty"` // error message if Status is "ERROR"
	Data   interface{} `json:"data,omitempty"`  // for additional data
}

func NewClient(nodeAddr string) *Client {
	return &Client{
		nodeAddr: nodeAddr,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Set(key, value string) error {
	data := map[string]string{"value": value}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	url := fmt.Sprintf("%s%s/kv/%s", c.nodeAddr, APIPrefix, key)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	if response.Status != "OK" {
		return fmt.Errorf("server error: %s", response.Error)
	}

	log.Printf("Set operation successful for key: %s", key)
	return nil
}

func (c *Client) Get(key string) (string, error) {
	url := fmt.Sprintf("%s%s/kv/%s", c.nodeAddr, APIPrefix, key)
	resp, err := c.client.Get(url)
	if err != nil {
		return "", fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to decode response: %v", err)
	}

	if response.Status != "OK" {
		return "", fmt.Errorf("server error: %s", response.Error)
	}

	log.Printf("Get operation successful for key: %s, value: %s", key, response.Value)
	return response.Value, nil
}

func (c *Client) Delete(key string) error {
	url := fmt.Sprintf("%s%s/kv/%s", c.nodeAddr, APIPrefix, key)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %v", err)
	}

	if response.Status != "OK" {
		return fmt.Errorf("server error: %s", response.Error)
	}

	log.Printf("Delete operation successful for key: %s", key)
	return nil
}

func (c *Client) GetClusterStatus() (map[string]interface{}, error) {
	url := fmt.Sprintf("%s%s/cluster/status", c.nodeAddr, APIPrefix)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	if response.Status != "OK" {
		return nil, fmt.Errorf("server error: %s", response.Error)
	}

	status, ok := response.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	return status, nil
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  set <key> <value>    - Set a key-value pair")
	fmt.Println("  get <key>           - Get value for a key")
	fmt.Println("  delete <key>        - Delete a key-value pair")
	fmt.Println("  status              - Get cluster status")
	fmt.Println("  help                - Show this help message")
	fmt.Println("  exit                - Exit the client")
}

func main() {
	nodeAddr := flag.String("node", DefaultNodeAddr, "Node address")
	flag.Parse()

	client := NewClient(*nodeAddr)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("CloudKVStore Client")
	fmt.Println("Type 'help' for available commands")
	fmt.Println("Type 'exit' to quit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])
		switch command {
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			if err := client.Set(parts[1], parts[2]); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			value, err := client.Get(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Value: %s\n", value)
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			if err := client.Delete(parts[1]); err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "status":
			status, err := client.GetClusterStatus()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				prettyJSON, _ := json.MarshalIndent(status, "", "  ")
				fmt.Println(string(prettyJSON))
			}

		case "help":
			printHelp()

		case "exit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
