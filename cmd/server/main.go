//go:build duckdb_arrow

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	duckserver "github.com/prochac/duckflight/internal/server"
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

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))

	addr := "0.0.0.0:8815"
	if err := server.Init(addr); err != nil {
		log.Fatalf("failed to init server: %v", err)
	}

	fmt.Printf("DuckFlight SQL server listening on %s\n", addr)

	go server.Serve()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
	server.Shutdown()
}
