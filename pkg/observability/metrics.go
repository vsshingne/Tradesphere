package observability

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	// Global Prometheus Metrics
	OrdersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradesphere_orders_total",
		Help: "The total number of orders placed",
	})
	TradesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradesphere_trades_total",
		Help: "The total number of executed trades",
	})
	WebsocketClientsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tradesphere_websocket_clients_active",
		Help: "The number of active websocket connections",
	})
	DBQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradesphere_db_query_duration_seconds",
		Help:    "Duration of database queries in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
	ReservationFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradesphere_reservation_failures_total",
		Help: "The total number of balance reservation failures",
	})
	HttpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradesphere_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path", "status"})
)

// Init sets up OpenTelemetry tracing and starts the metrics server on the given port (usually :9090 or part of the main mux)
func Init(serviceName string) func(context.Context) error {
	// Tracing Setup
	ctx := context.Background()
	
	// Check if OTLP endpoint is provided
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "jaeger:4318" // Default within our Helm chart
	}

	exp, err := otlptracehttp.New(ctx, 
		otlptracehttp.WithEndpoint(otlpEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Printf("Failed to create OTLP trace exporter: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)
	
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown
}

// MetricsHandler returns the prometheus HTTP handler
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// HTTPMiddleware wraps an HTTP handler to record metrics and traces
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		tracer := otel.Tracer("http-server")
		ctx, span := tracer.Start(r.Context(), r.Method+" "+r.URL.Path)
		defer span.End()

		rw := &responseWriter{w, http.StatusOK}
		next.ServeHTTP(rw, r.WithContext(ctx))

		duration := time.Since(start).Seconds()
		statusStr := http.StatusText(rw.status)
		if statusStr == "" {
			statusStr = "Unknown"
		}
		
		HttpRequestDuration.WithLabelValues(r.Method, r.URL.Path, statusStr).Observe(duration)
	})
}
