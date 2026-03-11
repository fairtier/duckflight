//go:build duckdb_arrow

package main

import (
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var level slog.LevelVar
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		if err := level.UnmarshalText([]byte(lvl)); err != nil {
			slog.Error("invalid LOG_LEVEL, using INFO", slog.String("value", lvl), slog.String("error", err.Error()))
		}
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &level})))

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

	server := flight.NewServerWithMiddleware([]flight.ServerMiddleware{duckserver.GRPCLoggingMiddleware()}, middleware...)
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
		mux.Handle("/metrics", promhttp.Handler())
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
