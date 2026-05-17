"""Shared fixtures for DuckFlight SQLAlchemy/ADBC integration tests.

Two client adapters expose a uniform `.execute(sql, params=None)` interface so
the same test body can run against:

  1. raw ADBC FlightSQL DBAPI (`adbc_driver_flightsql.dbapi`)
  2. SQLAlchemy via the gizmosql dialect
     (`superset-sqlalchemy-gizmosql-adbc-dialect`)

The DuckFlight server is reachable at ``$DUCKFLIGHT_HOST:$DUCKFLIGHT_PORT``,
exported by the Go test harness via ``host.testcontainers.internal``.
"""

from __future__ import annotations

import os
from typing import Any, Iterator, Sequence

import pytest

HOST = os.environ.get("DUCKFLIGHT_HOST", "host.testcontainers.internal")
PORT = int(os.environ.get("DUCKFLIGHT_PORT", "31337"))
USER = os.environ.get("DUCKFLIGHT_USER", "duckflight")
PASSWORD = os.environ.get("DUCKFLIGHT_PASSWORD", "duckflight")


class Client:
    """Uniform interface across raw ADBC and SQLAlchemy gizmosql dialect."""

    name: str

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        raise NotImplementedError

    def begin_transaction(self):
        """Returns a context manager that runs the body inside an explicit txn."""
        raise NotImplementedError

    def close(self) -> None:
        pass


class RawADBC(Client):
    name = "raw_adbc"

    def __init__(self, host: str, port: int) -> None:
        from adbc_driver_flightsql import dbapi as flight_dbapi

        uri = f"grpc+tcp://{host}:{port}"
        db_kwargs: dict[str, str] = {}
        if USER:
            # "username"/"password" are the canonical ADBC keys that the
            # Flight SQL driver maps onto AuthenticateBasicToken.
            db_kwargs["username"] = USER
            db_kwargs["password"] = PASSWORD
        # autocommit=True so ADBC doesn't open implicit Flight transactions
        # under us — we want SQL-level BEGIN/COMMIT/ROLLBACK to route directly
        # to the session-pinned DuckDB connection.
        self.conn = flight_dbapi.connect(uri, db_kwargs=db_kwargs, autocommit=True)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(sql, parameters=params or [])
            try:
                return list(cur.fetchall())
            except Exception:
                # Statement did not return rows (DDL/DML).
                return []

    def begin_transaction(self):
        return _RawADBCTxn(self.conn)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class _RawADBCTxn:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        # ADBC DBAPI: disable autocommit to enter manual transaction mode.
        self.conn.adbc_connection.set_autocommit(False)
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.adbc_connection.set_autocommit(True)
        return False


class SAGizmoSQL(Client):
    name = "gizmosql_dialect"

    def __init__(self, host: str, port: int) -> None:
        from sqlalchemy import create_engine

        # `useEncryption=False` skips TLS; matches the in-process DuckFlight
        # under test (no TLS termination).
        uri = (
            f"gizmosql://{USER}:{PASSWORD}@{host}:{port}"
            "?useEncryption=False&disableCertificateVerification=True"
        )
        self.engine = create_engine(uri, pool_pre_ping=False)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        from sqlalchemy import text

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if result.returns_rows:
                return [tuple(row) for row in result.fetchall()]
            return []

    def begin_transaction(self):
        return _SATxn(self.engine)

    def close(self) -> None:
        self.engine.dispose()


class _SATxn:
    def __init__(self, engine):
        self.engine = engine
        self._cm = None
        self.conn = None

    def __enter__(self):
        self._cm = self.engine.begin()
        self.conn = self._cm.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._cm.__exit__(exc_type, exc, tb)


def _make_client(name: str) -> Client:
    if name == "raw_adbc":
        return RawADBC(HOST, PORT)
    if name == "gizmosql_dialect":
        return SAGizmoSQL(HOST, PORT)
    raise ValueError(f"unknown client: {name}")


@pytest.fixture(params=["raw_adbc", "gizmosql_dialect"])
def client(request) -> Iterator[Client]:
    c = _make_client(request.param)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def raw_adbc() -> Iterator[RawADBC]:
    c = RawADBC(HOST, PORT)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def gizmosql_dialect() -> Iterator[SAGizmoSQL]:
    c = SAGizmoSQL(HOST, PORT)
    try:
        yield c
    finally:
        c.close()
