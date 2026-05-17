"""End-to-end tests against DuckFlight from the Python ecosystem.

Most tests run against both clients (raw ADBC + gizmosql SQLAlchemy dialect)
via the parameterized ``client`` fixture in ``conftest.py``.

A few tests are scoped to ``raw_adbc`` only because they rely on a *single*
underlying ADBC connection being reused across operations — verifying that
DuckDB connection-local state (TEMP tables, SET) survives across RPCs
because the server pins one DuckDB connection per Flight session. The
SQLAlchemy dialect's ``engine.connect()`` cycles checked-out connections
through SQLAlchemy's QueuePool, so each ``execute`` call may land on a
different ADBC connection — i.e. a different server-side session.

The DuckFlight server is bootstrapped in-process by the Go harness with a
small seeded dataset (see ``test/sqlalchemy_integration_test.go``).
"""

from __future__ import annotations

from sqlalchemy import text


def test_select_one(client):
    """The original Superset bug reproducer.

    ``engine.connect() → SELECT 1 → __exit__`` triggers SQLAlchemy's default
    "begin once" pattern, which issues a ROLLBACK on context exit. Before the
    session-pinned-connection fix this would error with "cannot rollback - no
    transaction is active"; now it lands on the same DuckDB connection where
    the implicit BEGIN ran, so DuckDB accepts it.
    """
    rows = client.execute("SELECT 1")
    assert rows == [(1,)]


def test_select_many_rows(client):
    """Regression test for v0.0.12 Arrow buffer aliasing — a multi-batch
    string-heavy result set."""
    rows = client.execute(
        "SELECT i, 'row_' || i::VARCHAR AS name "
        "FROM range(1000) t(i) ORDER BY i"
    )
    assert len(rows) == 1000
    assert rows[0] == (0, "row_0")
    assert rows[-1] == (999, "row_999")
    # Confirm string contents survive past the reader's Release.
    assert all(name == f"row_{i}" for i, name in rows)


def test_select_with_strings_and_nulls(client):
    rows = client.execute(
        "SELECT * FROM (VALUES "
        "(1, 'alpha'), (2, NULL), (3, 'gamma')) AS t(id, name) ORDER BY id"
    )
    assert rows == [(1, "alpha"), (2, None), (3, "gamma")]


def test_seeded_table_select(client):
    """Reads the seed table created by the Go harness."""
    rows = client.execute("SELECT id, name FROM sqla_seed ORDER BY id")
    assert rows == [(1, "first"), (2, "second"), (3, "third")]


def test_explicit_transaction_commit_raw_adbc(raw_adbc):
    """Explicit transaction via ADBC's autocommit=False toggle (the
    ``set_autocommit`` path, which goes through Flight ``BeginTransaction``).
    Uses a TEMP table — now works because the Flight txn binds to the
    session's pinned DuckDB connection."""
    table = "sqla_txn_commit_raw"
    with raw_adbc.begin_transaction():
        raw_adbc.execute(f"CREATE TEMP TABLE {table} (x INT)")
        raw_adbc.execute(f"INSERT INTO {table} VALUES (1), (2), (3)")
        rows = raw_adbc.execute(f"SELECT COUNT(*) FROM {table}")
        assert rows == [(3,)]


def test_explicit_transaction_commit_sa(gizmosql_dialect):
    """SQLAlchemy ``engine.begin()`` — exercises the dialect's
    ``do_begin``/``do_commit`` SQL-level BEGIN/COMMIT path."""
    with gizmosql_dialect.engine.begin() as conn:
        conn.execute(text("CREATE TEMP TABLE sqla_txn_commit_sa (x INT)"))
        conn.execute(text("INSERT INTO sqla_txn_commit_sa VALUES (1), (2), (3)"))
        rows = conn.execute(text("SELECT COUNT(*) FROM sqla_txn_commit_sa")).fetchall()
        assert rows == [(3,)]


def test_limit_zero_schema_inference(client):
    """Superset issues ``SELECT * FROM t LIMIT 0`` to introspect column types."""
    rows = client.execute("SELECT * FROM sqla_seed LIMIT 0")
    assert rows == []


def test_bare_rollback_after_begin(raw_adbc):
    """The Superset reproducer's SQL substrate: ``BEGIN`` then ``ROLLBACK``
    issued as bare SQL. Before the session-pinning fix the two RPCs would
    land on different pool connections and DuckDB would reject ``ROLLBACK``;
    now they share the session's pinned connection.

    Scoped to raw_adbc because the SA gizmosql dialect runs its own
    ``do_begin``/``do_rollback`` around every connection check-out and
    catches the historical error in ``fetchall`` — covered separately by
    ``test_sqlalchemy_connect_begin_once``.
    """
    raw_adbc.execute("BEGIN")
    raw_adbc.execute("ROLLBACK")
    rows = raw_adbc.execute("SELECT 1")
    assert rows == [(1,)]


# ---------------------------------------------------------------------------
# Single-connection tests (raw ADBC reuses one connection; SQLAlchemy's pool
# cycles connections per ``engine.connect()`` call, so these wouldn't be
# meaningful for the dialect fixture).
# ---------------------------------------------------------------------------


def test_temp_table_survives_across_rpcs(raw_adbc):
    """TEMP table is created in one RPC and read in the next — passes because
    DuckFlight pins one DuckDB connection per Flight session."""
    raw_adbc.execute("CREATE TEMP TABLE sqla_temp_across (id INT)")
    raw_adbc.execute("INSERT INTO sqla_temp_across VALUES (1), (2)")
    rows = raw_adbc.execute("SELECT COUNT(*) FROM sqla_temp_across")
    assert rows == [(2,)]


def test_set_variable_sticks(raw_adbc):
    """A SET in one RPC must affect the next — verifies session-pinned state."""
    raw_adbc.execute("SET memory_limit='256MB'")
    rows = raw_adbc.execute("SELECT current_setting('memory_limit')")
    # DuckDB normalizes memory limits to MiB; tolerate "256.0 MiB" / "256.0MiB".
    assert "MiB" in rows[0][0]


# ---------------------------------------------------------------------------
# SQLAlchemy-only tests (exercise the dialect's metadata / inspector layer).
# ---------------------------------------------------------------------------


def test_inspector_lists_seed_table(gizmosql_dialect):
    """Superset's table-listing path. Exercises DoGetTables."""
    from sqlalchemy import inspect

    insp = inspect(gizmosql_dialect.engine)
    tables = insp.get_table_names()
    assert "sqla_seed" in tables


def test_inspector_columns_for_seed_table(gizmosql_dialect):
    """Schema reflection — exercises buildBatchTableSchemas. Regression for
    the v0.0.11 SqlInfo corruption pattern."""
    from sqlalchemy import inspect

    insp = inspect(gizmosql_dialect.engine)
    cols = insp.get_columns("sqla_seed")
    names = [c["name"] for c in cols]
    assert names == ["id", "name"]


def test_sqlalchemy_connect_begin_once(gizmosql_dialect):
    """Direct SQLAlchemy ``engine.connect()`` context manager — the exact
    pattern that triggered the original bug from Superset."""
    with gizmosql_dialect.engine.connect() as conn:
        result = conn.execute(text("SELECT 42"))
        assert result.fetchall() == [(42,)]
    # __exit__ issues ROLLBACK; must not raise.


def test_prepared_statement_with_params(raw_adbc):
    """Prepared statement bind parameters round-trip through DoPut/DoGet."""
    with raw_adbc.conn.cursor() as cur:
        cur.execute("SELECT ? + ?", parameters=[1, 2])
        rows = cur.fetchall()
    assert rows == [(3,)]
