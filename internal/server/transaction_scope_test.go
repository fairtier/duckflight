//go:build duckdb_arrow

package server_test

import (
	"context"
)

// TestPreparedStatementHonorsTransaction is the regression test for prepared
// statements escaping their transaction. The Flight SQL transaction_id on
// ActionCreatePreparedStatementRequest used to be ignored, so an INSERT
// prepared inside a transaction executed on an unrelated pool connection in
// autocommit: the write was durable immediately and the client's rollback was
// a no-op that still reported success.
func (s *DuckFlightSQLSuite) TestPreparedStatementHonorsTransaction() {
	ctx := context.Background()

	s.EqualValues(4, s.execCountQuery())

	txn, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	stmt, err := txn.Prepare(ctx, "INSERT INTO intTable (keyName, value) VALUES ('txn_scoped', 7)")
	s.Require().NoError(err)

	n, err := stmt.ExecuteUpdate(ctx)
	s.Require().NoError(err)
	s.EqualValues(1, n)

	s.Require().NoError(stmt.Close(ctx))
	s.Require().NoError(txn.Rollback(ctx))

	// The insert must be gone. If the prepared statement ran on a different
	// connection in autocommit, the row survives the rollback.
	s.EqualValues(4, s.execCountQuery(), "rollback did not undo the prepared insert")
}

// TestPreparedStatementCommitInTransaction is the matching positive case: the
// same insert must survive a commit.
func (s *DuckFlightSQLSuite) TestPreparedStatementCommitInTransaction() {
	ctx := context.Background()

	txn, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	stmt, err := txn.Prepare(ctx, "INSERT INTO intTable (keyName, value) VALUES ('txn_committed', 8)")
	s.Require().NoError(err)

	_, err = stmt.ExecuteUpdate(ctx)
	s.Require().NoError(err)
	s.Require().NoError(stmt.Close(ctx))
	s.Require().NoError(txn.Commit(ctx))

	s.EqualValues(5, s.execCountQuery())
}

// TestPreparedSchemaProbeDoesNotExecute covers the schema probe running the
// statement it was only meant to describe. Appending " LIMIT 0" to the query
// puts the suffix inside a trailing line comment, so a DELETE would execute in
// full at prepare time — and again at execution.
func (s *DuckFlightSQLSuite) TestPreparedSchemaProbeDoesNotExecute() {
	ctx := context.Background()

	s.EqualValues(4, s.execCountQuery())

	stmt, err := s.client.Prepare(ctx, "DELETE FROM intTable WHERE value = 1 --trailing comment")
	s.Require().NoError(err)
	defer func() { _ = stmt.Close(ctx) }()

	// Preparing alone must not have deleted anything.
	s.EqualValues(4, s.execCountQuery(), "preparing the statement executed it")
}
