package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
)

func main() {
	// Open the Go file
	fileName := "node/src/internal/api/partition_manager.go" // replace with your Go file path
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new scanner to tokenize the file.
	fs := token.NewFileSet()

	// Parse the Go file into an abstract syntax tree (AST)
	node, err := parser.ParseFile(fs, fileName, file, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	// Walk through the AST and find function declarations
	ast.Inspect(node, func(n ast.Node) bool {
		// Look for function declarations (ast.FuncDecl)
		if fn, ok := n.(*ast.FuncDecl); ok {
			// Print the function name
			fmt.Printf("Function: %s\n", fn.Name.Name)

			// Print the parameters (inputs)
			fmt.Print("Parameters: (")
			for i, param := range fn.Type.Params.List {
				for _, paramName := range param.Names {
					fmt.Printf("%s", paramName.Name)
				}
				if i < len(fn.Type.Params.List)-1 {
					fmt.Print(", ")
				}
			}
			fmt.Println(")")

			// Print the return types
			if fn.Type.Results != nil {
				fmt.Print("Returns: (")
				for i, result := range fn.Type.Results.List {
					// Print the type of the result
					fmt.Printf("%s", result.Type)
					if i < len(fn.Type.Results.List)-1 {
						fmt.Print(", ")
					}
				}
				fmt.Println(")")
			} else {
				fmt.Println("Returns: (none)")
			}
			fmt.Println()
		}
		return true
	})
}
