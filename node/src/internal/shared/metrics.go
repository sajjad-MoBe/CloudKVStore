package shared

import (
	"sync"
	"time"
)

// MetricType represents the type of metric
type MetricType int

const (
	Counter MetricType = iota
	Gauge
	Histogram
)

// Metric represents a single metric
type Metric struct {
	Name      string
	Type      MetricType
	Value     float64
	Labels    map[string]string
	Timestamp time.Time
}

// MetricsCollector collects and reports system metrics
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*Metric
}

var (
	// DefaultCollector is the default metrics collector instance
	DefaultCollector *MetricsCollector
)

func init() {
	DefaultCollector = NewMetricsCollector()
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]*Metric),
	}
}

// RecordMetric records a new metric value
func (mc *MetricsCollector) RecordMetric(name string, metricType MetricType, value float64, labels map[string]string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics[name] = &Metric{
		Name:      name,
		Type:      metricType,
		Value:     value,
		Labels:    labels,
		Timestamp: time.Now(),
	}
}

// GetMetric retrieves a metric by name
func (mc *MetricsCollector) GetMetric(name string) *Metric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.metrics[name]
}

// GetAllMetrics retrieves all metrics
func (mc *MetricsCollector) GetAllMetrics() map[string]*Metric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	metrics := make(map[string]*Metric)
	for k, v := range mc.metrics {
		metrics[k] = v
	}
	return metrics
}

// Common metric names
const (
	MetricNodeHealth       = "node_health"
	MetricPartitionHealth  = "partition_health"
	MetricReplicationLag   = "replication_lag"
	MetricOperationLatency = "operation_latency"
	MetricOperationCount   = "operation_count"
	MetricPartitionSize    = "partition_size"
	MetricNodeLoad         = "node_load"
	MetricWALSize          = "wal_size"
	MetricWALOperations    = "wal_operations"
	MetricFailoverCount    = "failover_count"
	MetricRebalanceCount   = "rebalance_count"
	MetricReplicationCount = "replication_count"

	// WAL metrics
	MetricWALWriteCount    = "wal_write_count"
	MetricWALReadCount     = "wal_read_count"
	MetricWALRotationCount = "wal_rotation_count"
)
