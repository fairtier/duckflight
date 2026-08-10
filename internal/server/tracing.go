//go:build duckdb_arrow

package server

import (
	"go.opentelemetry.io/otel/attribute"
)

// Span attribute keys. The `db.*` ones follow the OpenTelemetry database
// conventions so generic trace UIs render them; the `flight.*` ones are
// Flight SQL specifics that have no conventional counterpart.
//
// None of these ever become *metric* attributes: statement handles, session
// ids and transaction ids are unbounded, and one time series per query would
// take the metrics pipeline down.
const (
	attrDBSystem = "db.system"
	// attrDBOperation is the coarse verb — "query", "update", "ingest",
	// "commit" — not the SQL text. For the text see [statementAttr], which is
	// DEBUG-gated because statements carry credentials and PII.
	attrDBOperation = "db.operation"
	// attrConnDirty records whether the statement left connection-local state
	// behind, which is what makes the pool recycle the connection instead of
	// reusing it. Explains connection churn without reading the SQL.
	attrConnDirty = "db.connection.dirty"
	// attrConnSource is which rung of acquireConn's precedence served this
	// call: transaction, session or pool. The single most useful attribute
	// when a client complains that its temp table or open transaction is not
	// visible.
	attrConnSource = "db.connection.source"

	attrStatementHandle = "flight.statement_handle"
	attrTransactionID   = "flight.transaction_id"
	attrPreparedHandle  = "flight.prepared_statement_handle"
	attrMetadataKind    = "flight.metadata_kind"
	attrRows            = "flight.rows"
	attrBytes           = "flight.bytes"
	attrSessionID       = "session.id"
)

// Connection sources, as reported by [attrConnSource].
const (
	connSourceTransaction = "transaction"
	connSourceSession     = "session"
	connSourcePool        = "pool"
)

// dbSystem marks every span that touches DuckDB, so spans from this service
// are separable from any other database work in the same trace.
var dbSystem = attribute.String(attrDBSystem, "duckdb")
