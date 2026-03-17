//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

func (s *DuckFlightSQLServer) GetFlightInfoPrimaryKeys(
	_ context.Context,
	_ flightsql.TableRef,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (s *DuckFlightSQLServer) DoGetPrimaryKeys(
	ctx context.Context,
	cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := `SELECT
		database_name AS catalog_name,
		schema_name AS db_schema_name,
		table_name,
		unnest(constraint_column_names) AS column_name,
		CAST(generate_subscripts(constraint_column_names, 1) AS INTEGER) AS key_sequence,
		NULL::VARCHAR AS key_name
	FROM duckdb_constraints()
	WHERE constraint_type = 'PRIMARY KEY'` +
		" AND " + catalogFilter("database_name", cmd.Catalog) +
		" AND " + schemaFilter("schema_name", cmd.DBSchema) +
		fmt.Sprintf(" AND table_name = '%s'", escapeSQLString(cmd.Table)) +
		" ORDER BY catalog_name, db_schema_name, table_name, key_sequence"

	return s.streamMetadata(ctx, query, schema_ref.PrimaryKeys)
}
