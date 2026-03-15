//go:build duckdb_arrow

package server

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
)

// XDBC/SQL type constants (java.sql.Types values).
const (
	xdbcBigInt    int32 = -5
	xdbcBit       int32 = -7
	xdbcBlob      int32 = 2004
	xdbcBoolean   int32 = 16
	xdbcDate      int32 = 91
	xdbcDecimal   int32 = 3
	xdbcDouble    int32 = 8
	xdbcFloat     int32 = 6
	xdbcInteger   int32 = 4
	xdbcSmallInt  int32 = 5
	xdbcTime      int32 = 92
	xdbcTimestamp int32 = 93
	xdbcTinyInt   int32 = -6
	xdbcVarBinary int32 = -3
	xdbcVarChar   int32 = 12
)

// Nullable: 1 = nullable. Searchable: 3 = searchable (all predicates).
const (
	xdbcNullable   int32 = 1
	xdbcSearchable int32 = 3
)

type xdbcType struct {
	typeName      string
	dataType      int32
	columnSize    *int32
	literalPrefix *string
	literalSuffix *string
	nullable      int32
	caseSensitive bool
	searchable    int32
	unsigned      *bool
	fixedPrecScale bool
	autoIncrement *bool
	sqlDataType   int32
}

func ptr[T any](v T) *T { return &v }

var xdbcTypes = []xdbcType{
	{typeName: "BIGINT", dataType: xdbcBigInt, columnSize: ptr[int32](19), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcBigInt},
	{typeName: "BIT", dataType: xdbcBit, columnSize: ptr[int32](1), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcBit},
	{typeName: "BLOB", dataType: xdbcBlob, columnSize: ptr[int32](2147483647), literalPrefix: ptr("X'"), literalSuffix: ptr("'"), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcBlob},
	{typeName: "BOOLEAN", dataType: xdbcBoolean, columnSize: ptr[int32](1), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcBoolean},
	{typeName: "DATE", dataType: xdbcDate, columnSize: ptr[int32](10), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcDate},
	{typeName: "DECIMAL", dataType: xdbcDecimal, columnSize: ptr[int32](38), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), fixedPrecScale: true, sqlDataType: xdbcDecimal},
	{typeName: "DOUBLE", dataType: xdbcDouble, columnSize: ptr[int32](15), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcDouble},
	{typeName: "FLOAT", dataType: xdbcFloat, columnSize: ptr[int32](7), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcFloat},
	{typeName: "HUGEINT", dataType: xdbcDecimal, columnSize: ptr[int32](38), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcDecimal},
	{typeName: "INTEGER", dataType: xdbcInteger, columnSize: ptr[int32](10), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcInteger},
	{typeName: "INTERVAL", dataType: xdbcVarChar, columnSize: ptr[int32](25), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcVarChar},
	{typeName: "SMALLINT", dataType: xdbcSmallInt, columnSize: ptr[int32](5), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcSmallInt},
	{typeName: "TIME", dataType: xdbcTime, columnSize: ptr[int32](15), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcTime},
	{typeName: "TIMESTAMP", dataType: xdbcTimestamp, columnSize: ptr[int32](26), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcTimestamp},
	{typeName: "TIMESTAMP WITH TIME ZONE", dataType: xdbcTimestamp, columnSize: ptr[int32](32), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcTimestamp},
	{typeName: "TINYINT", dataType: xdbcTinyInt, columnSize: ptr[int32](3), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(false), sqlDataType: xdbcTinyInt},
	{typeName: "UBIGINT", dataType: xdbcBigInt, columnSize: ptr[int32](20), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(true), sqlDataType: xdbcBigInt},
	{typeName: "UHUGEINT", dataType: xdbcDecimal, columnSize: ptr[int32](39), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(true), sqlDataType: xdbcDecimal},
	{typeName: "UINTEGER", dataType: xdbcInteger, columnSize: ptr[int32](10), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(true), sqlDataType: xdbcInteger},
	{typeName: "USMALLINT", dataType: xdbcSmallInt, columnSize: ptr[int32](5), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(true), sqlDataType: xdbcSmallInt},
	{typeName: "UTINYINT", dataType: xdbcTinyInt, columnSize: ptr[int32](3), nullable: xdbcNullable, searchable: xdbcSearchable, unsigned: ptr(true), sqlDataType: xdbcTinyInt},
	{typeName: "UUID", dataType: xdbcVarChar, columnSize: ptr[int32](36), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcVarChar},
	{typeName: "VARCHAR", dataType: xdbcVarChar, columnSize: ptr[int32](2147483647), literalPrefix: ptr("'"), literalSuffix: ptr("'"), nullable: xdbcNullable, caseSensitive: true, searchable: xdbcSearchable, sqlDataType: xdbcVarChar},
	{typeName: "VARBINARY", dataType: xdbcVarBinary, columnSize: ptr[int32](2147483647), nullable: xdbcNullable, searchable: xdbcSearchable, sqlDataType: xdbcVarBinary},
}

func (s *DuckFlightSQLServer) GetFlightInfoXdbcTypeInfo(
	_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *DuckFlightSQLServer) DoGetXdbcTypeInfo(
	_ context.Context, cmd flightsql.GetXdbcTypeInfo,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filterType := cmd.GetDataType()

	bldr := array.NewRecordBuilder(s.Alloc, schema_ref.XdbcTypeInfo)
	defer bldr.Release()

	typeNameBldr := bldr.Field(0).(*array.StringBuilder)      // type_name
	dataTypeBldr := bldr.Field(1).(*array.Int32Builder)       // data_type
	colSizeBldr := bldr.Field(2).(*array.Int32Builder)        // column_size
	litPrefixBldr := bldr.Field(3).(*array.StringBuilder)     // literal_prefix
	litSuffixBldr := bldr.Field(4).(*array.StringBuilder)     // literal_suffix
	createParamsBldr := bldr.Field(5).(*array.ListBuilder)    // create_params
	nullableBldr := bldr.Field(6).(*array.Int32Builder)       // nullable
	caseSensBldr := bldr.Field(7).(*array.BooleanBuilder)     // case_sensitive
	searchableBldr := bldr.Field(8).(*array.Int32Builder)     // searchable
	unsignedBldr := bldr.Field(9).(*array.BooleanBuilder)     // unsigned_attribute
	fixedPrecBldr := bldr.Field(10).(*array.BooleanBuilder)   // fixed_prec_scale
	autoIncBldr := bldr.Field(11).(*array.BooleanBuilder)     // auto_increment
	localNameBldr := bldr.Field(12).(*array.StringBuilder)    // local_type_name
	minScaleBldr := bldr.Field(13).(*array.Int32Builder)      // minimum_scale
	maxScaleBldr := bldr.Field(14).(*array.Int32Builder)      // maximum_scale
	sqlDataTypeBldr := bldr.Field(15).(*array.Int32Builder)   // sql_data_type
	dtSubcodeBldr := bldr.Field(16).(*array.Int32Builder)     // datetime_subcode
	numPrecRadBldr := bldr.Field(17).(*array.Int32Builder)    // num_prec_radix
	intervalPrecBldr := bldr.Field(18).(*array.Int32Builder)  // interval_precision

	for _, t := range xdbcTypes {
		if filterType != nil && *filterType != t.dataType {
			continue
		}

		typeNameBldr.Append(t.typeName)
		dataTypeBldr.Append(t.dataType)

		if t.columnSize != nil {
			colSizeBldr.Append(*t.columnSize)
		} else {
			colSizeBldr.AppendNull()
		}
		if t.literalPrefix != nil {
			litPrefixBldr.Append(*t.literalPrefix)
		} else {
			litPrefixBldr.AppendNull()
		}
		if t.literalSuffix != nil {
			litSuffixBldr.Append(*t.literalSuffix)
		} else {
			litSuffixBldr.AppendNull()
		}

		createParamsBldr.AppendNull() // no create_params
		nullableBldr.Append(t.nullable)
		caseSensBldr.Append(t.caseSensitive)
		searchableBldr.Append(t.searchable)

		if t.unsigned != nil {
			unsignedBldr.Append(*t.unsigned)
		} else {
			unsignedBldr.AppendNull()
		}
		fixedPrecBldr.Append(t.fixedPrecScale)
		if t.autoIncrement != nil {
			autoIncBldr.Append(*t.autoIncrement)
		} else {
			autoIncBldr.AppendNull()
		}

		localNameBldr.Append(t.typeName)
		minScaleBldr.AppendNull()
		maxScaleBldr.AppendNull()
		sqlDataTypeBldr.Append(t.sqlDataType)
		dtSubcodeBldr.AppendNull()

		switch t.dataType {
		case xdbcFloat, xdbcDouble:
			numPrecRadBldr.Append(2)
		case xdbcDecimal:
			numPrecRadBldr.Append(10)
		default:
			numPrecRadBldr.AppendNull()
		}
		intervalPrecBldr.AppendNull()
	}

	batch := bldr.NewRecordBatch()
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.XdbcTypeInfo, ch, nil
}
