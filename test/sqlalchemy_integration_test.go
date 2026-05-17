//go:build duckdb_arrow && sqlalchemy_integration

package duckflight_test

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/docker/docker/api/types/container"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	duckserver "github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testAuthUser   = "duckflight"
	testAuthPass   = "duckflight"
	testJWTSecret  = "test-secret-not-for-prod-32-bytes!"
)

// ---------------------------------------------------------------------------
// Embedded Python test files
// ---------------------------------------------------------------------------

//go:embed python/requirements.txt
var pyRequirements string

//go:embed python/conftest.py
var pyConftest string

//go:embed python/test_sqlalchemy.py
var pyTestSQLAlchemy string

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

type SQLAlchemySuite struct {
	suite.Suite

	server flight.Server
	client *flightsql.Client
	port   int
}

func TestSQLAlchemySuite(t *testing.T) {
	suite.Run(t, new(SQLAlchemySuite))
}

func (s *SQLAlchemySuite) SetupSuite() {
	// 1. Boot DuckFlight in-process on a free port. Enable basic auth so the
	//    gizmosql dialect (which always issues a Handshake with basic creds
	//    and expects a token in the response) has something to authenticate
	//    against — the dialect treats a missing Authorization header on the
	//    handshake response as a hard error.
	// PoolSize is generous because ADBC's DBAPI defaults to autocommit=False,
	// which opens an implicit Flight transaction per connection — each holds
	// a pool slot for its lifetime. SQLAlchemy's QueuePool default is 5, and
	// we run multiple test fixtures in series.
	//
	// QueryTimeout drives the prepared-statement reaper TTL (resourceTTL =
	// 2 * QueryTimeout). Generous so slow CI doesn't reap mid-test.
	srv, err := duckserver.New(&config.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   2,
		QueryTimeout: "5m",
		PoolSize:     32,
	})
	s.Require().NoError(err)

	authMW, err := auth.Middleware(auth.Config{
		Users:     map[string]string{testAuthUser: testAuthPass},
		JWTSecret: []byte(testJWTSecret),
		JWTTTL:    time.Hour,
	})
	s.Require().NoError(err)

	var middlewares []flight.ServerMiddleware
	if authMW != nil {
		middlewares = append(middlewares, *authMW)
	}

	s.server = flight.NewServerWithMiddleware(middlewares)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("0.0.0.0:0"))
	go func() { _ = s.server.Serve() }()

	addr := s.server.Addr().String()
	_, portStr, err := splitHostPort(addr)
	s.Require().NoError(err, "addr=%s", addr)
	s.port, err = strconv.Atoi(portStr)
	s.Require().NoError(err)

	// 2. Seed canonical test data the Python tests reference.
	ctx := context.Background()
	s.Require().NoError(duckserver.SeedSQL(ctx, `
		CREATE OR REPLACE TABLE sqla_seed (id INT, name VARCHAR);
		INSERT INTO sqla_seed VALUES (1, 'first'), (2, 'second'), (3, 'third');
	`))
}

func (s *SQLAlchemySuite) TearDownSuite() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
}

// ---------------------------------------------------------------------------
// The one test: run the pytest suite inside a Python container.
// ---------------------------------------------------------------------------

func (s *SQLAlchemySuite) TestPytestSuite() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Entrypoint script — install deps then run pytest. We install the
	// gizmosql dialect with --no-deps because its pyproject pulls in the
	// entire apache-superset stack as an install requirement even though the
	// dialect's runtime imports are just adbc + sqlalchemy.
	pytestFilter := ""
	if f := os.Getenv("SQLA_PYTEST_FILTER"); f != "" {
		pytestFilter = " -k '" + f + "'"
	}
	entrypoint := `set -eu
cd /tests
pip install --no-cache-dir -r requirements.txt
pip install --no-cache-dir --no-deps 'superset-sqlalchemy-gizmosql-adbc-dialect==0.0.10'
python -c "from superset_sqlalchemy_gizmosql_adbc_dialect import GizmoSQLDialect; print('dialect import OK:', GizmoSQLDialect)"
pytest -v --tb=short --color=no` + pytestFilter + `
`

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "python:3.12-slim",
			// Run in the host's network namespace so localhost reaches the
			// in-process DuckFlight server directly. Linux-only — CI is Linux.
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.NetworkMode = "host"
			},
			Env: map[string]string{
				"DUCKFLIGHT_HOST":               "127.0.0.1",
				"DUCKFLIGHT_PORT":               strconv.Itoa(s.port),
				"DUCKFLIGHT_USER":               testAuthUser,
				"DUCKFLIGHT_PASSWORD":           testAuthPass,
				"PIP_DISABLE_PIP_VERSION_CHECK": "1",
				"PYTHONDONTWRITEBYTECODE":       "1",
				"PYTHONUNBUFFERED":              "1",
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            strings.NewReader(pyRequirements),
					ContainerFilePath: "/tests/requirements.txt",
					FileMode:          0o644,
				},
				{
					Reader:            strings.NewReader(pyConftest),
					ContainerFilePath: "/tests/conftest.py",
					FileMode:          0o644,
				},
				{
					Reader:            strings.NewReader(pyTestSQLAlchemy),
					ContainerFilePath: "/tests/test_sqlalchemy.py",
					FileMode:          0o644,
				},
			},
			Entrypoint: []string{"bash", "-c", entrypoint},
			WaitingFor: wait.ForExit().WithExitTimeout(8 * time.Minute),
		},
		Started: true,
	})
	s.Require().NoError(err, "start pytest container")
	defer func() {
		_ = c.Terminate(context.Background())
	}()

	state, err := c.State(ctx)
	s.Require().NoError(err)

	// Always dump container logs so the user can read pytest output.
	logs, logErr := c.Logs(ctx)
	if logErr == nil {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, logs)
		_ = logs.Close()
		s.T().Logf("--- pytest container logs ---\n%s", buf.String())
	}

	s.Equal(0, state.ExitCode, "pytest failed (exit %d)", state.ExitCode)
}

// splitHostPort splits an "addr:port" string. Works for IPv4 and "[::]:port".
func splitHostPort(addr string) (host, port string, err error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("no port in %q", addr)
	}
	return addr[:idx], addr[idx+1:], nil
}
