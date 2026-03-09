//go:build duckdb_arrow

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/prochac/duckflight/internal/auth"
	duckserver "github.com/prochac/duckflight/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	srv, err := duckserver.New(duckserver.Config{
		MemoryLimit:  "1GB",
		MaxThreads:   4,
		QueryTimeout: "30s",
		PoolSize:     8,
	})
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

	addr := "0.0.0.0:8815"
	if err := server.Init(addr); err != nil {
		log.Fatalf("failed to init server: %v", err)
	}

	// Metrics endpoint
	metricAddr := "0.0.0.0:9090"
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

