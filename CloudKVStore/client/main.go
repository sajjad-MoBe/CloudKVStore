package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

type SetRequestBody struct {
    Value string `json:"value"`
}

type Response struct {
    Status string `json:"status"`
    Value  string `json:"value"`
    Error  string `json:"error"`
}

func main() {
    serverAddr := flag.String("server", "http://localhost:8080", "Server address")
    flag.Parse()

    if len(flag.Args()) < 1 {
        printUsage()
        os.Exit(1)
    }

    client := &http.Client{}
    command := flag.Arg(0)

    switch command {
    case "set":
        if len(flag.Args()) != 3 {
            fmt.Println("Usage: client set <key> <value>")
            os.Exit(1)
        }
        key := flag.Arg(1)
        value := flag.Arg(2)
        if err := setKey(client, *serverAddr, key, value); err != nil {
            log.Fatal(err)
        }

    case "get":
        if len(flag.Args()) != 2 {
            fmt.Println("Usage: client get <key>")
            os.Exit(1)
        }
        key := flag.Arg(1)
        if err := getKey(client, *serverAddr, key); err != nil {
            log.Fatal(err)
        }

    case "delete":
        if len(flag.Args()) != 2 {
            fmt.Println("Usage: client delete <key>")
            os.Exit(1)
        }
        key := flag.Arg(1)
        if err := deleteKey(client, *serverAddr, key); err != nil {
            log.Fatal(err)
        }

    case "viewwal":
        if err := viewWAL(client, *serverAddr); err != nil {
            log.Fatal(err)
        }

    default:
        fmt.Printf("Unknown command: %s\n", command)
        printUsage()
        os.Exit(1)
    }
}

func viewWAL(client *http.Client, serverAddr string) error {
    req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wal", serverAddr), nil)
    if err != nil {
        return fmt.Errorf("error creating request: %v", err)
    }

    return executeRequest(client, req)
}

func printUsage() {
    fmt.Println("Usage:")
    fmt.Println("  client --server http://localhost:8080 set <key> <value>")
    fmt.Println("  client --server http://localhost:8080 get <key>")
    fmt.Println("  client --server http://localhost:8080 delete <key>")
    fmt.Println("  client --server http://localhost:8080 viewwal")
}

func setKey(client *http.Client, serverAddr, key, value string) error {
    body := SetRequestBody{Value: value}
    jsonBody, err := json.Marshal(body)
    if err != nil {
        return fmt.Errorf("error marshaling request: %v", err)
    }

    req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/kv/%s", serverAddr, key), bytes.NewBuffer(jsonBody))
    if err != nil {
        return fmt.Errorf("error creating request: %v", err)
    }
    req.Header.Set("Content-Type", "application/json")

    return executeRequest(client, req)
}

func getKey(client *http.Client, serverAddr, key string) error {
    req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/kv/%s", serverAddr, key), nil)
    if err != nil {
        return fmt.Errorf("error creating request: %v", err)
    }

    return executeRequest(client, req)
}

func deleteKey(client *http.Client, serverAddr, key string) error {
    req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/kv/%s", serverAddr, key), nil)
    if err != nil {
        return fmt.Errorf("error creating request: %v", err)
    }

    return executeRequest(client, req)
}

func executeRequest(client *http.Client, req *http.Request) error {
    resp, err := client.Do(req)
    if err != nil {
        return fmt.Errorf("error executing request: %v", err)
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return fmt.Errorf("error reading response: %v", err)
    }

    var response Response
    if err := json.Unmarshal(body, &response); err != nil {
        // If we can't parse the JSON, just print the raw response
        fmt.Printf("Status: %d\n", resp.StatusCode)
        fmt.Printf("Response: %s\n", string(body))
        return nil
    }

    // Pretty print the response
    fmt.Printf("Status: %d\n", resp.StatusCode)
    if response.Status != "" {
        fmt.Printf("Operation Status: %s\n", response.Status)
    }
    if response.Value != "" {
        fmt.Printf("Value: %s\n", response.Value)
    }
    if response.Error != "" {
        fmt.Printf("Error: %s\n", response.Error)
    }

    return nil
}
