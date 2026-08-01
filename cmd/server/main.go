//go:build duckdb_arrow

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/ratelimit"
	duckserver "github.com/fairtier/duckflight/internal/server"
	"github.com/fairtier/duckflight/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	// shutdownGrace bounds how long in-flight RPCs get to finish before the
	// server is stopped the hard way.
	shutdownGrace = 20 * time.Second
	// maxConcurrentStreams caps the streams one connection may open. Health
	// RPCs — including the streaming Watch — bypass auth, so without a cap an
	// unauthenticated peer can pin a goroutine per stream indefinitely.
	maxConcurrentStreams = 256
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
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
		return err
	}
	defer func() {
		shCtx, shCancel := context.WithTimeout(context.Background(), time.Second*3)
		defer shCancel()
		if err := tel.Shutdown(shCtx); err != nil {
			slog.Error("telemetry shutdown error", slog.String("error", err.Error()))
		}
	}()

	// Wire slog to OTel log SDK via otelslog bridge.
	// The LoggerProvider always exports to stderr; when OTLP is configured it also exports to the collector.
	slog.SetDefault(slog.New(newLevelHandler(&level,
		otelslog.NewHandler("duckflight",
			otelslog.WithLoggerProvider(tel.LoggerProvider()),
		),
	)))
	// Initialize OTel metrics.
	meter := otel.GetMeterProvider().Meter("duckflight")
	duckserver.InitMetrics(meter)
	ratelimit.InitMetrics(meter)

	poolSize, err := envInt("POOL_SIZE", 8)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}
	if poolSize < 1 {
		slog.Error("POOL_SIZE must be at least 1", slog.Int("value", poolSize))
		return fmt.Errorf("POOL_SIZE must be at least 1, got %d", poolSize)
	}
	maxThreads, err := envInt("MAX_THREADS", 4)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}
	maxResultBytes, err := envInt64("MAX_RESULT_BYTES", 0)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}

	cfg := &config.Config{
		MemoryLimit:         envOr("MEMORY_LIMIT", "1GB"),
		MaxThreads:          maxThreads,
		QueryTimeout:        envOr("QUERY_TIMEOUT", "30s"),
		PoolSize:            poolSize,
		MaxResultBytes:      maxResultBytes,
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
		ExtensionDir:        os.Getenv("EXTENSION_DIR"),
	}

	srv, err := duckserver.New(cfg)
	if err != nil {
		slog.Error("failed to create server", slog.String("error", err.Error()))
		return err
	}

	users, err := parseUsers(os.Getenv("AUTH_USERS"))
	if err != nil {
		slog.Error("invalid AUTH_USERS", slog.String("error", err.Error()))
		return err
	}
	jwtTTL, err := envDuration("AUTH_JWT_TTL", time.Hour)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}
	authCfg := auth.Config{
		Users:        users,
		JWTSecret:    []byte(os.Getenv("AUTH_JWT_SECRET")),
		JWTTTL:       jwtTTL,
		StaticTokens: parseCSV(os.Getenv("AUTH_TOKENS")),
	}
	if iss := os.Getenv("OIDC_ISSUER"); iss != "" {
		v, err := auth.NewOIDCVerifier(ctx, iss, os.Getenv("OIDC_AUDIENCE"))
		if err != nil {
			slog.Error("oidc setup failed", slog.String("issuer", iss), slog.String("error", err.Error()))
			return err
		}
		authCfg.OIDC = v
	}
	authMW, err := auth.Middleware(authCfg)
	if err != nil {
		slog.Error("auth middleware setup failed", slog.String("error", err.Error()))
		return err
	}

	// Rate limit middleware — set RATE_LIMIT_RPS env var to enable.
	rateLimitRPS, err := envFloat64("RATE_LIMIT_RPS", 0)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}
	rateLimitBurst, err := envInt("RATE_LIMIT_BURST", 0)
	if err != nil {
		slog.Error("invalid configuration", slog.String("error", err.Error()))
		return err
	}

	// Middleware order: recovery (outer) → logging → rate limit → auth (inner).
	// Recovery is outermost so a panic anywhere below it fails one call
	// instead of the process. Rate limiting sits above auth so that the
	// expensive part of authentication — signature verification, JWKS
	// lookups — is itself subject to the limit.
	middlewares := []flight.ServerMiddleware{
		duckserver.RecoveryMiddleware(),
		duckserver.GRPCLoggingMiddleware(),
	}
	if m := ratelimit.Middleware(rateLimitRPS, rateLimitBurst); m != nil {
		middlewares = append(middlewares, *m)
	}
	if authMW != nil {
		middlewares = append(middlewares, *authMW)
	}

	grpcOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithFilter(filters.Not(filters.HealthCheck())),
		)),
		// Bound what one unauthenticated peer can pin: health RPCs (including
		// the streaming Watch) bypass auth, and each open stream holds a
		// goroutine. Keepalive enforcement drops connections that go silent.
		grpc.MaxConcurrentStreams(uint32(maxConcurrentStreams)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
	}

	certFile, keyFile := os.Getenv("TLS_CERT"), os.Getenv("TLS_KEY")
	// Half-configured TLS is a misconfiguration, not a request for plaintext.
	// Treating it as "TLS not requested" would silently put bearer tokens and
	// query results on the wire in the clear.
	if (certFile == "") != (keyFile == "") {
		slog.Error("TLS_CERT and TLS_KEY must be set together")
		return fmt.Errorf("TLS_CERT and TLS_KEY must be set together")
	}
	tlsEnabled := certFile != ""
	if tlsEnabled {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			slog.Error("failed to load TLS certificate", slog.String("error", err.Error()))
			return err
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

		if caFile := os.Getenv("TLS_CA"); caFile != "" {
			caPEM, err := os.ReadFile(caFile)
			if err != nil {
				slog.Error("failed to read TLS CA", slog.String("error", err.Error()))
				return err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				slog.Error("failed to parse TLS CA certificate")
				return fmt.Errorf("failed to parse TLS CA certificate")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}

		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		slog.Info("TLS enabled", slog.String("cert", certFile), slog.String("key", keyFile))
	}

	// One unambiguous line about the security posture, so an open server is
	// never something you have to infer from a missing log entry.
	if authMW == nil {
		slog.Warn("AUTH DISABLED — every network peer can execute arbitrary SQL; set AUTH_USERS, AUTH_TOKENS or OIDC_ISSUER")
	} else {
		slog.Info("auth enabled", slog.String("backends", strings.Join(authCfg.Backends(), ",")))
		if !tlsEnabled {
			slog.Warn("auth is enabled but TLS is not; credentials and bearer tokens cross the network in cleartext")
		}
	}

	server := flight.NewServerWithMiddleware(middlewares, grpcOpts...)
	server.RegisterFlightService(flightsql.NewFlightServer(duckserver.NewLoggingServer(srv)))
	reflection.Register(server)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthSrv)

	addr := envOr("LISTEN_ADDR", "0.0.0.0:31337")
	if err := server.Init(addr); err != nil {
		slog.Error("failed to init server", slog.String("error", err.Error()))
		return err
	}

	// Metrics endpoint
	metricAddr := envOr("METRIC_ADDR", "0.0.0.0:9090")
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(tel.Gatherer, promhttp.HandlerOpts{}))
	metricSrv := &http.Server{
		Addr:    metricAddr,
		Handler: mux,
		// Without a header deadline a peer can hold connections open by
		// dribbling out request headers.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		slog.Info("metrics server listening", slog.String("addr", metricAddr))
		if err := metricSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server error", slog.String("error", err.Error()))
		}
	}()

	slog.Info("DuckFlight SQL server listening", slog.String("addr", addr))

	// serveErr also signals shutdown: if Serve returns, gRPC is dead and the
	// process must exit so the orchestrator restarts it. Blocking on the
	// signal alone would leave a process that answers health checks with
	// SERVING while serving nothing.
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var runErr error
	select {
	case runErr = <-serveErr:
		if runErr != nil {
			slog.Error("server error", slog.String("error", runErr.Error()))
		} else {
			slog.Error("gRPC server stopped serving unexpectedly")
			runErr = fmt.Errorf("gRPC server stopped serving")
		}
	case <-sig:
		slog.Info("shutting down")
	}

	// Stop reporting healthy first, so load balancers drain us before the
	// in-flight streams are cut.
	healthSrv.Shutdown()

	// arrow-go only exposes GracefulStop, which waits on in-flight RPCs with no
	// deadline of its own — one stalled DoGet would otherwise keep the process
	// alive until the pod is SIGKILLed, and a second SIGTERM would be swallowed.
	stopped := make(chan struct{})
	go func() {
		server.Shutdown()
		close(stopped)
	}()

	graceful := true
	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		slog.Warn("graceful shutdown timed out", slog.Duration("after", shutdownGrace))
		graceful = false
	case <-sig:
		slog.Warn("second signal received during shutdown")
		graceful = false
	}

	shCtx, shCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer shCancel()
	if err := metricSrv.Shutdown(shCtx); err != nil {
		slog.Error("metrics server shutdown error", slog.String("error", err.Error()))
	}

	if !graceful {
		// Stop waiting and let the process exit. Deliberately skipping
		// srv.Close(): closing DuckDB connections underneath streams that are
		// still running them would crash in CGo, and there is nothing left to
		// preserve — exiting reclaims everything an in-memory engine holds.
		slog.Warn("exiting without waiting for in-flight streams")
		return errForcedShutdown
	}

	// Closes the session manager, the background reapers and the DuckDB engine.
	if err := srv.Close(); err != nil {
		slog.Error("server close error", slog.String("error", err.Error()))
	}
	return runErr
}

// errForcedShutdown reports that shutdown gave up on in-flight RPCs, so the
// exit status distinguishes a clean drain from a forced one.
var errForcedShutdown = errors.New("shutdown forced before in-flight RPCs completed")

// Every env parser below reports malformed input rather than falling back to
// the default. Several of these values are protections — RATE_LIMIT_RPS,
// MAX_RESULT_BYTES — and silently reading a typo as "0" turns them off with no
// signal at all.

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not an integer", key, v)
	}
	return n, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not an integer", key, v)
	}
	return n, nil
}

func envFloat64(key string, fallback float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a number", key, v)
	}
	return f, nil
}

func envBool(key string) bool {
	v := os.Getenv(key)
	return v == "true" || v == "1"
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration", key, v)
	}
	return d, nil
}

func parseCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseUsers parses AUTH_USERS ("user:pass,user2:pass2"). A malformed entry is
// an error rather than a skipped line: dropping it silently can leave zero
// users parsed, which disables the whole auth layer and starts a fully open
// server on the strength of one missing colon.
//
// Entries are never echoed — `AUTH_USERS=":hunter2"` would otherwise write the
// password to stderr and on to the OTLP collector.
func parseUsers(v string) (map[string]string, error) {
	if v == "" {
		return nil, nil
	}
	out := make(map[string]string)
	for i, pair := range strings.Split(v, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		user, pass, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("AUTH_USERS entry %d is missing the ':' separator", i+1)
		}
		if user == "" {
			return nil, fmt.Errorf("AUTH_USERS entry %d has an empty username", i+1)
		}
		out[user] = pass
	}
	if len(out) == 0 {
		return nil, errors.New("AUTH_USERS is set but contains no usable entries")
	}
	return out, nil
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
