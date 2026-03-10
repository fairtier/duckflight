//go:build duckdb_arrow

package main

import (
	"fmt"
	"log"
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
		log.Fatalf("failed to create server: %v", err)
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

	server := flight.NewServerWithMiddleware(nil, middleware...)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))

	addr := envOr("LISTEN_ADDR", "0.0.0.0:31337")
	if err := server.Init(addr); err != nil {
		log.Fatalf("failed to init server: %v", err)
	}

	// Metrics endpoint
	metricAddr := envOr("METRIC_ADDR", "0.0.0.0:9090")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server listening on %s\n", metricAddr)
		if err := http.ListenAndServe(metricAddr, mux); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	fmt.Printf("DuckFlight SQL server listening on %s\n", addr)

	go server.Serve()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
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
