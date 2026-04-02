package types

import (
	"reflect"
	"time"
)

type (
	TaosBool      bool
	TaosTinyint   int8
	TaosSmallint  int16
	TaosInt       int32
	TaosBigint    int64
	TaosUTinyint  uint8
	TaosUSmallint uint16
	TaosUInt      uint32
	TaosUBigint   uint64
	TaosFloat     float32
	TaosDouble    float64
	TaosBinary    []byte
	TaosVarBinary []byte
	TaosNchar     string
	TaosTimestamp struct {
		T         time.Time
		Precision int
	}
	TaosJson     []byte
	TaosGeometry []byte
)

var (
	TaosBoolType      = reflect.TypeOf(TaosBool(false))
	TaosTinyintType   = reflect.TypeOf(TaosTinyint(0))
	TaosSmallintType  = reflect.TypeOf(TaosSmallint(0))
	TaosIntType       = reflect.TypeOf(TaosInt(0))
	TaosBigintType    = reflect.TypeOf(TaosBigint(0))
	TaosUTinyintType  = reflect.TypeOf(TaosUTinyint(0))
	TaosUSmallintType = reflect.TypeOf(TaosUSmallint(0))
	TaosUIntType      = reflect.TypeOf(TaosUInt(0))
	TaosUBigintType   = reflect.TypeOf(TaosUBigint(0))
	TaosFloatType     = reflect.TypeOf(TaosFloat(0))
	TaosDoubleType    = reflect.TypeOf(TaosDouble(0))
	TaosBinaryType    = reflect.TypeOf(TaosBinary(nil))
	TaosVarBinaryType = reflect.TypeOf(TaosVarBinary(nil))
	TaosNcharType     = reflect.TypeOf(TaosNchar(""))
	TaosTimestampType = reflect.TypeOf(TaosTimestamp{})
	TaosJsonType      = reflect.TypeOf(TaosJson(""))
	TaosGeometryType  = reflect.TypeOf(TaosGeometry(nil))
)

type ColumnType struct {
	Type   reflect.Type
	MaxLen int
}
