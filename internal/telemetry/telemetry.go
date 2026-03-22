package telemetry

import (
	"context"
	"fmt"
	"os"

	promclient "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config holds OpenTelemetry configuration.
type Config struct {
	OTLPEndpoint string // OTLP collector endpoint (empty = tracing disabled)
	ServiceName  string // Service name for spans
	Insecure     bool   // Use insecure gRPC to collector
}

// Telemetry holds initialized providers and the Prometheus gatherer.
type Telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
	Gatherer       promclient.Gatherer
}

// LoggerProvider returns the OTel LoggerProvider for use with otelslog bridge.
func (t *Telemetry) LoggerProvider() *sdklog.LoggerProvider {
	return t.loggerProvider
}

// Setup initializes OTel trace and metric providers.
func Setup(ctx context.Context, cfg Config) (*Telemetry, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "duckflight"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}

	t := &Telemetry{}

	// Tracing: only set up if endpoint is provided.
	if cfg.OTLPEndpoint != "" {
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		}
		exporter, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating trace exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		t.tracerProvider = tp
		otel.SetTracerProvider(tp)
	}

	// Metrics: always set up with Prometheus exporter.
	registry := promclient.NewRegistry()
	promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("creating prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	t.meterProvider = mp
	t.Gatherer = registry
	otel.SetMeterProvider(mp)

	// Logging: always export to stderr; additionally export via OTLP when endpoint is set.
	stderrExporter, err := stdoutlog.New(stdoutlog.WithWriter(os.Stderr))
	if err != nil {
		return nil, fmt.Errorf("creating stderr log exporter: %w", err)
	}
	lpOpts := []sdklog.LoggerProviderOption{
		sdklog.WithProcessor(sdklog.NewBatchProcessor(stderrExporter)),
		sdklog.WithResource(res),
	}
	if cfg.OTLPEndpoint != "" {
		logOpts := []otlploggrpc.Option{
			otlploggrpc.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.Insecure {
			logOpts = append(logOpts, otlploggrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		}
		otlpExporter, err := otlploggrpc.New(ctx, logOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating log exporter: %w", err)
		}
		lpOpts = append(lpOpts, sdklog.WithProcessor(sdklog.NewBatchProcessor(otlpExporter)))
	}
	lp := sdklog.NewLoggerProvider(lpOpts...)
	t.loggerProvider = lp
	global.SetLoggerProvider(lp)

	return t, nil
}

// Shutdown flushes and shuts down providers.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if t.loggerProvider != nil {
		if err := t.loggerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}
