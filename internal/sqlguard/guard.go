//go:build duckdb_arrow

// Package sqlguard decides, with DuckDB's own parser, whether client SQL may
// run under a policy — without running any of it against the database that
// would serve it.
//
// A [Guard] owns a private, throwaway DuckDB instance that holds no data, has
// no catalog attached, no secrets, and external access disabled. Everything
// the guard asks of DuckDB happens there: splitting a script into statements
// (duckdb_extract_statements), preparing a statement to learn its type, and
// json_serialize_sql to obtain the parsed AST of a SELECT. None of those reach
// the serving engine, so classification cannot execute, bind against, or
// leave state on a client's connection, and a table function's bind step
// (read_csv sniffing a file, for instance) cannot touch the filesystem or the
// network even while it is being classified.
package sqlguard

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	bindings "github.com/duckdb/duckdb-go-bindings"
	_ "github.com/duckdb/duckdb-go/v2" // registers the "duckdb" database/sql driver
)

// isolationConfig is applied to both private instances at open time, before
// any statement runs. The instances parse and prepare client SQL and nothing
// else, so none of these costs anything.
var isolationConfig = map[string]string{
	"enable_external_access":       "false",
	"allow_persistent_secrets":     "false",
	"autoinstall_known_extensions": "false",
	"autoload_known_extensions":    "false",
	"threads":                      "1",
	"memory_limit":                 "64MB",
	"lock_configuration":           "true",
}

// Guard classifies SQL. It is safe for concurrent use; calls are serialized,
// because a DuckDB connection is not, and parsing is cheap next to execution.
type Guard struct {
	mu     sync.Mutex
	closed bool

	// Statement splitting needs the C API's extracted-statement handles,
	// which duckdb-go does not expose, so that half talks to the bindings
	// directly.
	db   bindings.Database
	conn bindings.Connection

	// json_serialize_sql runs through database/sql on a second private
	// instance: it only needs a bound string parameter and a VARCHAR back.
	astDB *sql.DB
}

// New opens the guard's private DuckDB instances.
func New() (*Guard, error) {
	g := &Guard{}

	var cfg bindings.Config
	if bindings.CreateConfig(&cfg) == bindings.StateError {
		return nil, errors.New("sqlguard: create config")
	}
	defer bindings.DestroyConfig(&cfg)
	for k, v := range isolationConfig {
		if bindings.SetConfig(cfg, k, v) == bindings.StateError {
			return nil, fmt.Errorf("sqlguard: set config %s", k)
		}
	}
	var errMsg string
	if bindings.OpenExt("", &g.db, cfg, &errMsg) == bindings.StateError {
		return nil, fmt.Errorf("sqlguard: open: %s", errMsg)
	}
	if bindings.Connect(g.db, &g.conn) == bindings.StateError {
		bindings.Close(&g.db)
		return nil, errors.New("sqlguard: connect")
	}

	dsn := make([]string, 0, len(isolationConfig))
	for k, v := range isolationConfig {
		dsn = append(dsn, k+"="+v)
	}
	astDB, err := sql.Open("duckdb", "?"+strings.Join(dsn, "&"))
	if err != nil {
		g.closeLocked()
		return nil, fmt.Errorf("sqlguard: open ast instance: %w", err)
	}
	astDB.SetMaxOpenConns(1)
	astDB.SetMaxIdleConns(1)
	astDB.SetConnMaxLifetime(0)
	g.astDB = astDB
	// Fail at startup rather than on the first client statement if this
	// DuckDB build lacks the json extension that provides json_serialize_sql.
	if _, err := g.serialize(context.Background(), "SELECT 1"); err != nil {
		g.closeLocked()
		return nil, fmt.Errorf("sqlguard: json_serialize_sql unavailable: %w", err)
	}
	return g, nil
}

// Close releases the private instances. Safe to call more than once.
func (g *Guard) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeLocked()
}

func (g *Guard) closeLocked() {
	if g.closed {
		return
	}
	g.closed = true
	if g.astDB != nil {
		_ = g.astDB.Close()
	}
	bindings.Disconnect(&g.conn)
	bindings.Close(&g.db)
}

var errClosed = errors.New("sqlguard: closed")

// ExtensionStatements reports whether any statement in query — a single
// statement or a whole script — installs, loads or updates an extension.
//
// Each extracted statement is prepared on the guard's empty instance. A
// statement that references tables the guard doesn't have fails to prepare
// there; that is fine, because INSTALL, LOAD and UPDATE EXTENSIONS bind
// against nothing and always prepare. A script that cannot even be split
// (a syntax error) is reported as an error: the serving engine would refuse
// it too, and saying "no extension statement" about SQL nobody could parse
// would be a guess.
func (g *Guard) ExtensionStatements(query string) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false, errClosed
	}

	var stmts bindings.ExtractedStatements
	n := bindings.ExtractStatements(g.conn, query, &stmts)
	defer bindings.DestroyExtracted(&stmts)
	if n == 0 {
		if msg := bindings.ExtractStatementsError(stmts); msg != "" {
			return false, &ParseError{Msg: msg}
		}
		return false, nil // empty query: nothing to load
	}
	for i := bindings.IdxT(0); i < n; i++ {
		var ps bindings.PreparedStatement
		state := bindings.PrepareExtractedStatement(g.conn, stmts, i, &ps)
		var t bindings.StatementType
		if state != bindings.StateError {
			t = bindings.PreparedStatementType(ps)
		}
		bindings.DestroyPrepare(&ps)
		switch t {
		case bindings.StatementTypeLoad, bindings.StatementTypeUpdateExtensions:
			return true, nil
		}
	}
	return false, nil
}

// ParseError is DuckDB's parser rejecting the SQL outright. It is the
// client's syntax error, not a policy decision, and is reported as such.
type ParseError struct{ Msg string }

func (e *ParseError) Error() string { return e.Msg }

// Violation is a statement the read-only policy refuses. Reason is safe to
// return to the client: it names the offending construct, never echoes SQL.
type Violation struct{ Reason string }

func (e *Violation) Error() string { return "read-only session: " + e.Reason }

// astResult is json_serialize_sql's envelope.
type astResult struct {
	Error        bool              `json:"error"`
	ErrorType    string            `json:"error_type"`
	ErrorMessage string            `json:"error_message"`
	Statements   []json.RawMessage `json:"statements"`
}

// serialize returns DuckDB's AST for query. The query is bound as a
// parameter, never spliced into SQL.
func (g *Guard) serialize(ctx context.Context, query string) (*astResult, error) {
	var raw string
	if err := g.astDB.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", query).Scan(&raw); err != nil {
		return nil, err
	}
	var res astResult
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return nil, fmt.Errorf("sqlguard: decode ast: %w", err)
	}
	return &res, nil
}
