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

// fkBaseQuery flattens duckdb_constraints() into the Flight SQL
// imported/exported-keys shape.
//
// The primary-key side reuses the foreign-key side's catalog and schema
// because DuckDB only supports foreign keys between tables in the same schema,
// and duckdb_constraints() exposes just the referenced table name — there is
// no separate referenced catalog/schema to report.
//
// update_rule and delete_rule are reported as 3 (importedKeyNoAction) because
// DuckDB neither implements nor exposes referential actions.
const fkBaseQuery = `
WITH fks AS (
	SELECT
		fk.database_name AS fk_catalog,
		fk.schema_name AS fk_schema,
		fk.table_name AS fk_table,
		unnest(fk.constraint_column_names) AS fk_column,
		generate_subscripts(fk.constraint_column_names, 1) AS key_seq,
		fk.referenced_table AS pk_table,
		unnest(fk.referenced_column_names) AS pk_column,
		fk.constraint_name AS fk_key_name
	FROM duckdb_constraints() fk
	WHERE fk.constraint_type = 'FOREIGN KEY'
)
SELECT
	fk_catalog AS pk_catalog_name,
	fk_schema AS pk_db_schema_name,
	pk_table AS pk_table_name,
	pk_column AS pk_column_name,
	fk_catalog AS fk_catalog_name,
	fk_schema AS fk_db_schema_name,
	fk_table AS fk_table_name,
	fk_column AS fk_column_name,
	CAST(key_seq AS INTEGER) AS key_sequence,
	fk_key_name,
	NULL::VARCHAR AS pk_key_name,
	CAST(3 AS UTINYINT) AS update_rule,
	CAST(3 AS UTINYINT) AS delete_rule
FROM fks`

// --- Imported Keys (FK table → which PK tables does it reference?) ---

func (s *DuckFlightSQLServer) GetFlightInfoImportedKeys(
	_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (s *DuckFlightSQLServer) DoGetImportedKeys(
	ctx context.Context, cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := fkBaseQuery + " WHERE " + catalogFilter("fk_catalog", cmd.Catalog) +
		" AND " + schemaFilter("fk_schema", cmd.DBSchema) +
		fmt.Sprintf(" AND fk_table = '%s' ORDER BY pk_table, key_seq", escapeSQLString(cmd.Table))
	return s.streamMetadata(ctx, query, schema_ref.ImportedExportedKeysAndCrossReference)
}

// --- Exported Keys (PK table → which FK tables reference it?) ---

func (s *DuckFlightSQLServer) GetFlightInfoExportedKeys(
	_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

func (s *DuckFlightSQLServer) DoGetExportedKeys(
	ctx context.Context, cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := fkBaseQuery + " WHERE " + catalogFilter("fk_catalog", cmd.Catalog) +
		" AND " + schemaFilter("fk_schema", cmd.DBSchema) +
		fmt.Sprintf(" AND pk_table = '%s' ORDER BY fk_table, key_seq", escapeSQLString(cmd.Table))
	return s.streamMetadata(ctx, query, schema_ref.ImportedExportedKeysAndCrossReference)
}

// --- Cross Reference (specific PK table + FK table pair) ---

func (s *DuckFlightSQLServer) GetFlightInfoCrossReference(
	_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.CrossReference), nil
}

func (s *DuckFlightSQLServer) DoGetCrossReference(
	ctx context.Context, cmd flightsql.CrossTableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	pk := cmd.PKRef
	fk := cmd.FKRef
	// Both refs constrain the same row: DuckDB keeps a foreign key and the
	// table it references in one schema, so the catalog/schema given on either
	// side has to match. Applying only the PK ref's would silently ignore a
	// client that qualified the FK side instead.
	query := fkBaseQuery + " WHERE " + catalogFilter("fk_catalog", pk.Catalog) +
		" AND " + schemaFilter("fk_schema", pk.DBSchema) +
		" AND " + catalogFilter("fk_catalog", fk.Catalog) +
		" AND " + schemaFilter("fk_schema", fk.DBSchema) +
		fmt.Sprintf(" AND pk_table = '%s' AND fk_table = '%s' ORDER BY key_seq",
			escapeSQLString(pk.Table), escapeSQLString(fk.Table))
	return s.streamMetadata(ctx, query, schema_ref.ImportedExportedKeysAndCrossReference)
}
