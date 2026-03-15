//go:build duckdb_arrow

package server_test

import (
	"sync"

	promclient "github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"github.com/prochac/duckflight/internal/server"
)

var (
	testMetricsOnce sync.Once
	testGatherer    promclient.Gatherer
)

// ensureTestMetrics initializes OTel metrics exactly once for the test package.
func ensureTestMetrics() {
	testMetricsOnce.Do(func() {
		registry := promclient.NewRegistry()
		promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
		if err != nil {
			panic(err)
		}
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(promExporter))
		server.InitMetrics(mp.Meter("duckflight-test"))
		testGatherer = registry
	})
}
