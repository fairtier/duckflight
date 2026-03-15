//go:build duckdb_arrow

package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/prochac/duckflight/internal/auth"
	duckserver "github.com/prochac/duckflight/internal/server"
	"github.com/prochac/duckflight/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

func main() {
	var level slog.LevelVar
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		if err := level.UnmarshalText([]byte(lvl)); err != nil {
			slog.Error("invalid LOG_LEVEL, using INFO", slog.String("value", lvl), slog.String("error", err.Error()))
		}
	}

	ctx := context.Background()

	// Initialize OpenTelemetry (before slog wiring, since we need the LoggerProvider).
	tel, err := telemetry.Setup(ctx, telemetry.Config{
		OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:  envOr("OTEL_SERVICE_NAME", "duckflight"),
		Insecure:     envBool("OTEL_EXPORTER_OTLP_INSECURE"),
	})
	if err != nil {
		slog.Error("failed to setup telemetry", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Wire slog to OTel log SDK via otelslog bridge.
	slog.SetDefault(slog.New(newLevelHandler(&level,
		otelslog.NewHandler("duckflight",
			otelslog.WithLoggerProvider(tel.LoggerProvider()),
		),
	)))
	defer func() {
		if err := tel.Shutdown(ctx); err != nil {
			slog.Error("telemetry shutdown error", slog.String("error", err.Error()))
		}
	}()

	// Initialize OTel metrics.
	duckserver.InitMetrics(otel.GetMeterProvider().Meter("duckflight"))

	cfg := duckserver.Config{
		MemoryLimit:         envOr("MEMORY_LIMIT", "1GB"),
		MaxThreads:          envInt("MAX_THREADS", 4),
		QueryTimeout:        envOr("QUERY_TIMEOUT", "30s"),
		PoolSize:            envInt("POOL_SIZE", 8),
		MaxResultBytes:      envInt64("MAX_RESULT_BYTES", 0),
		IcebergEndpoint:     os.Getenv("ICEBERG_ENDPOINT"),
		IcebergWarehouse:    os.Getenv("ICEBERG_WAREHOUSE"),
		IcebergClientID:     os.Getenv("ICEBERG_CLIENT_ID"),
		IcebergClientSecret: os.Getenv("ICEBERG_CLIENT_SECRET"),
		IcebergOAuth2URI:    os.Getenv("ICEBERG_OAUTH2_URI"),
		S3Endpoint:          os.Getenv("S3_ENDPOINT"),
		S3AccessKey:         os.Getenv("S3_ACCESS_KEY"),
		S3SecretKey:         os.Getenv("S3_SECRET_KEY"),
		S3Region:            os.Getenv("S3_REGION"),
		S3URLStyle:          os.Getenv("S3_URL_STYLE"),
	}

	srv, err := duckserver.New(cfg)
	if err != nil {
		slog.Error("failed to create server", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Auth middleware — set AUTH_TOKENS env var (comma-separated) to enable.
	var authTokens []string
	if t := os.Getenv("AUTH_TOKENS"); t != "" {
		for _, tok := range strings.Split(t, ",") {
			tok = strings.TrimSpace(tok)
			if tok != "" {
				authTokens = append(authTokens, tok)
			}
		}
	}
	middleware := auth.BearerTokenMiddleware(authTokens)

	server := flight.NewServerWithMiddleware(
		[]flight.ServerMiddleware{duckserver.GRPCLoggingMiddleware()},
		append(middleware, grpc.StatsHandler(otelgrpc.NewServerHandler()))...,
	)
	server.RegisterFlightService(flightsql.NewFlightServer(duckserver.NewLoggingServer(srv)))

	addr := envOr("LISTEN_ADDR", "0.0.0.0:31337")
	if err := server.Init(addr); err != nil {
		slog.Error("failed to init server", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Metrics endpoint
	metricAddr := envOr("METRIC_ADDR", "0.0.0.0:9090")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(tel.Gatherer, promhttp.HandlerOpts{}))
		slog.Info("metrics server listening", slog.String("addr", metricAddr))
		if err := http.ListenAndServe(metricAddr, mux); err != nil {
			slog.Error("metrics server error", slog.String("error", err.Error()))
		}
	}()

	slog.Info("DuckFlight SQL server listening", slog.String("addr", addr))

	go func() {
		if err := server.Serve(); err != nil {
			slog.Error("server error", slog.String("error", err.Error()))
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("shutting down")
	server.Shutdown()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func envBool(key string) bool {
	v := os.Getenv(key)
	return v == "true" || v == "1"
}

// levelHandler filters log records below a minimum level.
type levelHandler struct {
	level slog.Leveler
	inner slog.Handler
}

func newLevelHandler(level slog.Leveler, inner slog.Handler) *levelHandler {
	return &levelHandler{level: level, inner: inner}
}

func (h *levelHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *levelHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithAttrs(attrs)}
}

func (h *levelHandler) WithGroup(name string) slog.Handler {
	return &levelHandler{level: h.level, inner: h.inner.WithGroup(name)}
}
