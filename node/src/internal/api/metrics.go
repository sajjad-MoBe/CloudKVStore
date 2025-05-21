package api

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics
type Metrics struct {
	// Request metrics
	requestDuration *prometheus.HistogramVec
	requestTotal    *prometheus.CounterVec
	requestErrors   *prometheus.CounterVec

	// Storage metrics
	storageSize    prometheus.Gauge
	storageKeys    prometheus.Gauge
	storageErrors  *prometheus.CounterVec
	storageLatency *prometheus.HistogramVec

	// Auth metrics
	authAttempts *prometheus.CounterVec
	authErrors   *prometheus.CounterVec
	activeUsers  prometheus.Gauge
	apiKeyUsage  *prometheus.CounterVec
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		// Request metrics
		requestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "Duration of HTTP requests in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "path", "status"},
		),
		requestTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		requestErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_request_errors_total",
				Help: "Total number of HTTP request errors",
			},
			[]string{"method", "path", "error_type"},
		),

		// Storage metrics
		storageSize: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_size_bytes",
				Help: "Total size of stored data in bytes",
			},
		),
		storageKeys: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "storage_keys_total",
				Help: "Total number of keys in storage",
			},
		),
		storageErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "storage_errors_total",
				Help: "Total number of storage errors",
			},
			[]string{"operation", "error_type"},
		),
		storageLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "storage_operation_duration_seconds",
				Help:    "Duration of storage operations in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		),

		// Auth metrics
		authAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "auth_attempts_total",
				Help: "Total number of authentication attempts",
			},
			[]string{"result"},
		),
		authErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "auth_errors_total",
				Help: "Total number of authentication errors",
			},
			[]string{"error_type"},
		),
		activeUsers: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "active_users_total",
				Help: "Total number of active users",
			},
		),
		apiKeyUsage: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "api_key_usage_total",
				Help: "Total number of API key usages",
			},
			[]string{"user_id", "role"},
		),
	}
}

// MetricsMiddleware adds Prometheus metrics to requests
func (m *Metrics) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrw := &responseWriter{ResponseWriter: w}

		next.ServeHTTP(wrw, r)

		duration := time.Since(start).Seconds()
		status := wrw.statusCode

		// Record request metrics
		m.requestDuration.WithLabelValues(r.Method, r.URL.Path, string(status)).Observe(duration)
		m.requestTotal.WithLabelValues(r.Method, r.URL.Path, string(status)).Inc()

		if status >= 400 {
			m.requestErrors.WithLabelValues(r.Method, r.URL.Path, string(status)).Inc()
		}
	})
}

// RecordStorageMetrics records storage operation metrics
func (m *Metrics) RecordStorageMetrics(operation string, duration time.Duration, err error) {
	m.storageLatency.WithLabelValues(operation).Observe(duration.Seconds())
	if err != nil {
		m.storageErrors.WithLabelValues(operation, err.Error()).Inc()
	}
}

// RecordAuthMetrics records authentication metrics
func (m *Metrics) RecordAuthMetrics(user *User, err error) {
	if err != nil {
		m.authErrors.WithLabelValues(err.Error()).Inc()
		m.authAttempts.WithLabelValues("failure").Inc()
		return
	}

	m.authAttempts.WithLabelValues("success").Inc()
	m.apiKeyUsage.WithLabelValues(user.ID, string(user.Roles[0])).Inc()
}

// UpdateStorageMetrics updates storage-related metrics
func (m *Metrics) UpdateStorageMetrics(size int64, keys int) {
	m.storageSize.Set(float64(size))
	m.storageKeys.Set(float64(keys))
}

// UpdateUserMetrics updates user-related metrics
func (m *Metrics) UpdateUserMetrics(activeUsers int) {
	m.activeUsers.Set(float64(activeUsers))
}
