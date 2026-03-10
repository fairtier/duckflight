//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// Query DuckDB's constraint info for primary keys.
	query := `SELECT
		database_name AS catalog_name,
		schema_name AS db_schema_name,
		table_name,
		unnest(constraint_column_names) AS column_name,
		generate_subscripts(constraint_column_names, 1) AS key_sequence
	FROM duckdb_constraints()
	WHERE constraint_type = 'PRIMARY KEY'`

	if cmd.Catalog != nil {
		query += fmt.Sprintf(" AND database_name = '%s'", escapeSQLString(*cmd.Catalog))
	}
	if cmd.DBSchema != nil {
		query += fmt.Sprintf(" AND schema_name = '%s'", escapeSQLString(*cmd.DBSchema))
	}
	query += fmt.Sprintf(" AND table_name = '%s'", escapeSQLString(cmd.Table))
	query += " ORDER BY catalog_name, db_schema_name, table_name, key_sequence"

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "query error: %s", err)
	}
	defer rdr.Release()

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.PrimaryKeys)
	defer bldr.Release()

	catalogBldr := bldr.Field(0).(*array.StringBuilder)
	schemaBldr := bldr.Field(1).(*array.StringBuilder)
	tableBldr := bldr.Field(2).(*array.StringBuilder)
	columnBldr := bldr.Field(3).(*array.StringBuilder)
	seqBldr := bldr.Field(4).(*array.Int32Builder)

	for rdr.Next() {
		rec := rdr.RecordBatch()
		catCol := rec.Column(0).(*array.String)
		schCol := rec.Column(1).(*array.String)
		tblCol := rec.Column(2).(*array.String)
		colCol := rec.Column(3).(*array.String)
		seqCol := rec.Column(4)

		for i := 0; i < int(rec.NumRows()); i++ {
			catalogBldr.Append(catCol.Value(i))
			schemaBldr.Append(schCol.Value(i))
			tableBldr.Append(tblCol.Value(i))
			columnBldr.Append(colCol.Value(i))
			// key_sequence is 1-based; DuckDB generate_subscripts returns int64
			switch col := seqCol.(type) {
			case *array.Int64:
				seqBldr.Append(int32(col.Value(i)))
			case *array.Int32:
				seqBldr.Append(col.Value(i))
			}
			bldr.Field(5).AppendNull() // key_name is always null
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "reader error: %s", err)
	}

	batch := bldr.NewRecordBatch()
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.PrimaryKeys, ch, nil
}
