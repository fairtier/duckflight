//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fkRow represents a single foreign key column mapping.
type fkRow struct {
	pkCatalog  string
	pkSchema   string
	pkTable    string
	pkColumn   string
	fkCatalog  string
	fkSchema   string
	fkTable    string
	fkColumn   string
	keySeq     int32
	fkKeyName  string
	pkKeyName  *string // always nil — DuckDB doesn't expose PK constraint name via FK metadata
	updateRule uint8
	deleteRule uint8
}

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
	fk_catalog, fk_schema,
	pk_table, pk_column,
	fk_table, fk_column,
	key_seq, fk_key_name
FROM fks`

func (s *DuckFlightSQLServer) queryForeignKeys(ctx context.Context, query string) ([]fkRow, error) {
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query error: %s", err)
	}
	defer rdr.Release()

	var rows []fkRow
	for rdr.Next() {
		rec := rdr.RecordBatch()
		fkCat := rec.Column(0).(*array.String)
		fkSch := rec.Column(1).(*array.String)
		pkTbl := rec.Column(2).(*array.String)
		pkCol := rec.Column(3).(*array.String)
		fkTbl := rec.Column(4).(*array.String)
		fkCol := rec.Column(5).(*array.String)
		keySq := rec.Column(6).(*array.Int64)
		fkNam := rec.Column(7).(*array.String)

		for i := 0; i < int(rec.NumRows()); i++ {
			// DuckDB stores FK in the same catalog/schema as the FK table;
			// the referenced (PK) table is assumed to be in the same catalog/schema.
			// Clone strings to ensure they outlive the Arrow record batch.
			rows = append(rows, fkRow{
				pkCatalog:  strings.Clone(fkCat.Value(i)),
				pkSchema:   strings.Clone(fkSch.Value(i)),
				pkTable:    strings.Clone(pkTbl.Value(i)),
				pkColumn:   strings.Clone(pkCol.Value(i)),
				fkCatalog:  strings.Clone(fkCat.Value(i)),
				fkSchema:   strings.Clone(fkSch.Value(i)),
				fkTable:    strings.Clone(fkTbl.Value(i)),
				fkColumn:   strings.Clone(fkCol.Value(i)),
				keySeq:     int32(keySq.Value(i)),
				fkKeyName:  strings.Clone(fkNam.Value(i)),
				updateRule: 3, // NO ACTION
				deleteRule: 3, // NO ACTION
			})
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "reader error: %s", err)
	}
	return rows, nil
}

func buildKeysRecordBatch(rows []fkRow) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.ImportedExportedKeysAndCrossReference)
	defer bldr.Release()

	pkCatBldr := bldr.Field(0).(*array.StringBuilder)
	pkSchBldr := bldr.Field(1).(*array.StringBuilder)
	pkTblBldr := bldr.Field(2).(*array.StringBuilder)
	pkColBldr := bldr.Field(3).(*array.StringBuilder)
	fkCatBldr := bldr.Field(4).(*array.StringBuilder)
	fkSchBldr := bldr.Field(5).(*array.StringBuilder)
	fkTblBldr := bldr.Field(6).(*array.StringBuilder)
	fkColBldr := bldr.Field(7).(*array.StringBuilder)
	seqBldr := bldr.Field(8).(*array.Int32Builder)
	fkNameBldr := bldr.Field(9).(*array.StringBuilder)
	pkNameBldr := bldr.Field(10).(*array.StringBuilder)
	updateBldr := bldr.Field(11).(*array.Uint8Builder)
	deleteBldr := bldr.Field(12).(*array.Uint8Builder)

	for _, r := range rows {
		pkCatBldr.Append(r.pkCatalog)
		pkSchBldr.Append(r.pkSchema)
		pkTblBldr.Append(r.pkTable)
		pkColBldr.Append(r.pkColumn)
		fkCatBldr.Append(r.fkCatalog)
		fkSchBldr.Append(r.fkSchema)
		fkTblBldr.Append(r.fkTable)
		fkColBldr.Append(r.fkColumn)
		seqBldr.Append(r.keySeq)
		fkNameBldr.Append(r.fkKeyName)
		pkNameBldr.AppendNull()
		updateBldr.Append(r.updateRule)
		deleteBldr.Append(r.deleteRule)
	}

	return bldr.NewRecordBatch()
}

func streamKeysBatch(rows []fkRow) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	batch := buildKeysRecordBatch(rows)
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return schema_ref.ImportedExportedKeysAndCrossReference, ch, nil
}

// --- Imported Keys (FK table → which PK tables does it reference?) ---

func (s *DuckFlightSQLServer) GetFlightInfoImportedKeys(
	_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (s *DuckFlightSQLServer) DoGetImportedKeys(
	ctx context.Context, cmd flightsql.TableRef,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := fkBaseQuery + fmt.Sprintf(
		" WHERE fk_catalog = '%s' AND fk_schema = '%s' AND fk_table = '%s' ORDER BY pk_table, key_seq",
		escapeSQLString(defaultCatalog(cmd.Catalog)),
		escapeSQLString(defaultSchema(cmd.DBSchema)),
		escapeSQLString(cmd.Table),
	)
	rows, err := s.queryForeignKeys(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	return streamKeysBatch(rows)
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
	query := fkBaseQuery + fmt.Sprintf(
		" WHERE fk_catalog = '%s' AND fk_schema = '%s' AND pk_table = '%s' ORDER BY fk_table, key_seq",
		escapeSQLString(defaultCatalog(cmd.Catalog)),
		escapeSQLString(defaultSchema(cmd.DBSchema)),
		escapeSQLString(cmd.Table),
	)
	rows, err := s.queryForeignKeys(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	return streamKeysBatch(rows)
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
	query := fkBaseQuery + fmt.Sprintf(
		" WHERE fk_catalog = '%s' AND fk_schema = '%s' AND pk_table = '%s' AND fk_table = '%s' ORDER BY key_seq",
		escapeSQLString(defaultCatalog(pk.Catalog)),
		escapeSQLString(defaultSchema(pk.DBSchema)),
		escapeSQLString(pk.Table),
		escapeSQLString(fk.Table),
	)
	rows, err := s.queryForeignKeys(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	return streamKeysBatch(rows)
}

func defaultCatalog(c *string) string {
	if c != nil {
		return *c
	}
	return "memory"
}

func defaultSchema(s *string) string {
	if s != nil {
		return *s
	}
	return "main"
}
