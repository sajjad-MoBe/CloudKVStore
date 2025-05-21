package benchmarks_test

import (
	"fmt"
	"testing"

	"github.com/sajjad-MoBe/CloudKVStore/client"
)

const lbAddress = "localhost:8000"

func BenchmarkWriteThroughput(b *testing.B) {
	c := client.NewClient(lbAddress, client.DefaultRetryConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		err := c.Set(key, make([]byte, 1024)) // 1KB values
		if err != nil {
			b.Fatal(err)
		}
	}
}

// In benchmarks/benchmarks_test.go
func BenchmarkBatchWrites(b *testing.B) {
	c := client.NewClient(lbAddress, client.DefaultRetryConfig())
	batch := client.NewBatchClient(c, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch.Set(fmt.Sprintf("key-%d", i), make([]byte, 1024))
	}

	results := batch.Wait()
	for _, res := range results {
		if res.Err != nil { // Now using exported Err field
			b.Errorf("Failed on key %s: %v", res.Key, res.Err) // Using exported Key field
		}
	}
}

func BenchmarkReadThroughput(b *testing.B) {
	client := client.NewClient(lbAddress, client.DefaultRetryConfig())

	// Pre-populate with test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		client.Set(key, make([]byte, 1024))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%1000)
		_, err := client.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
