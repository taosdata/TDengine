package common

import (
	"reflect"

	"github.com/taosdata/taosadapter/v3/driver/types"
)

var (
	NullInt8    = reflect.TypeOf(types.NullInt8{})
	NullInt16   = reflect.TypeOf(types.NullInt16{})
	NullInt32   = reflect.TypeOf(types.NullInt32{})
	NullInt64   = reflect.TypeOf(types.NullInt64{})
	NullUInt8   = reflect.TypeOf(types.NullUInt8{})
	NullUInt16  = reflect.TypeOf(types.NullUInt16{})
	NullUInt32  = reflect.TypeOf(types.NullUInt32{})
	NullUInt64  = reflect.TypeOf(types.NullUInt64{})
	NullFloat32 = reflect.TypeOf(types.NullFloat32{})
	NullFloat64 = reflect.TypeOf(types.NullFloat64{})
	NullTime    = reflect.TypeOf(types.NullTime{})
	NullBool    = reflect.TypeOf(types.NullBool{})
	NullString  = reflect.TypeOf(types.NullString{})
	Bytes       = reflect.TypeOf([]byte{})
	NullJson    = reflect.TypeOf(types.NullJson{})
	UnknownType = reflect.TypeOf(new(interface{})).Elem()
)

var ColumnTypeMap = map[int]reflect.Type{
	TSDB_DATA_TYPE_BOOL:      NullBool,
	TSDB_DATA_TYPE_TINYINT:   NullInt8,
	TSDB_DATA_TYPE_SMALLINT:  NullInt16,
	TSDB_DATA_TYPE_INT:       NullInt32,
	TSDB_DATA_TYPE_BIGINT:    NullInt64,
	TSDB_DATA_TYPE_UTINYINT:  NullUInt8,
	TSDB_DATA_TYPE_USMALLINT: NullUInt16,
	TSDB_DATA_TYPE_UINT:      NullUInt32,
	TSDB_DATA_TYPE_UBIGINT:   NullUInt64,
	TSDB_DATA_TYPE_FLOAT:     NullFloat32,
	TSDB_DATA_TYPE_DOUBLE:    NullFloat64,
	TSDB_DATA_TYPE_BINARY:    NullString,
	TSDB_DATA_TYPE_NCHAR:     NullString,
	TSDB_DATA_TYPE_TIMESTAMP: NullTime,
	TSDB_DATA_TYPE_JSON:      NullJson,
	TSDB_DATA_TYPE_VARBINARY: Bytes,
	TSDB_DATA_TYPE_GEOMETRY:  Bytes,
}
