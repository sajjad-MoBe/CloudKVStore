package api

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer manages distributed tracing
type Tracer struct {
	tracer trace.Tracer
}

// NewTracer creates a new tracer
func NewTracer(serviceName string, endpoint string) (*Tracer, error) {
	// Create Jaeger exporter
	exporter, err := jaeger.New(
		jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)),
	)
	if err != nil {
		return nil, err
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Tracer{
		tracer: tp.Tracer(serviceName),
	}, nil
}

// TracingMiddleware adds tracing to requests
func (t *Tracer) TracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		spanName := r.URL.Path

		// Extract trace context from request
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))

		// Start span
		ctx, span := t.tracer.Start(ctx, spanName)
		defer span.End()

		// Add request attributes
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.url", r.URL.String()),
			attribute.String("http.user_agent", r.UserAgent()),
		)

		// Create response writer that captures status code
		wrw := &responseWriter{ResponseWriter: w}

		// Process request
		next.ServeHTTP(wrw, r.WithContext(ctx))

		// Add response attributes
		span.SetAttributes(
			attribute.Int("http.status_code", wrw.statusCode),
		)
	})
}

// StartSpan starts a new span
func (t *Tracer) StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name)
}

// AddSpanEvent adds an event to the current span
func (t *Tracer) AddSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

// AddSpanError adds an error to the current span
func (t *Tracer) AddSpanError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// TraceStorageOperation traces a storage operation
func (t *Tracer) TraceStorageOperation(ctx context.Context, operation string, fn func(context.Context) error) error {
	ctx, span := t.tracer.Start(ctx, "storage."+operation)
	defer span.End()

	start := time.Now()
	err := fn(ctx)
	duration := time.Since(start)

	span.SetAttributes(
		attribute.String("operation", operation),
		attribute.String("duration", duration.String()),
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

// TraceAuthOperation traces an authentication operation
func (t *Tracer) TraceAuthOperation(ctx context.Context, operation string, fn func(context.Context) error) error {
	ctx, span := t.tracer.Start(ctx, "auth."+operation)
	defer span.End()

	start := time.Now()
	err := fn(ctx)
	duration := time.Since(start)

	span.SetAttributes(
		attribute.String("operation", operation),
		attribute.String("duration", duration.String()),
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
