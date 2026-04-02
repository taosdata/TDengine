package taosWS

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/types"
)

func assertValueEqual(t *testing.T, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected value:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestConvertInsertValue_AllFieldTypesAndErrors(t *testing.T) {
	ts := time.Unix(1700000000, 123000000).UTC()
	tests := []struct {
		name      string
		fieldType int8
		value     driver.Value
		precision int
		expect    driver.Value
	}{
		{name: "nil", fieldType: common.TSDB_DATA_TYPE_INT, value: nil, expect: nil},
		{name: "null_type", fieldType: common.TSDB_DATA_TYPE_NULL, value: int64(1), expect: nil},
		{name: "bool", fieldType: common.TSDB_DATA_TYPE_BOOL, value: true, expect: true},
		{name: "tinyint", fieldType: common.TSDB_DATA_TYPE_TINYINT, value: int16(7), expect: int8(7)},
		{name: "smallint", fieldType: common.TSDB_DATA_TYPE_SMALLINT, value: int32(8), expect: int16(8)},
		{name: "int", fieldType: common.TSDB_DATA_TYPE_INT, value: int64(9), expect: int32(9)},
		{name: "bigint", fieldType: common.TSDB_DATA_TYPE_BIGINT, value: int32(10), expect: int64(10)},
		{name: "utinyint", fieldType: common.TSDB_DATA_TYPE_UTINYINT, value: uint16(11), expect: uint8(11)},
		{name: "usmallint", fieldType: common.TSDB_DATA_TYPE_USMALLINT, value: uint32(12), expect: uint16(12)},
		{name: "uint", fieldType: common.TSDB_DATA_TYPE_UINT, value: uint64(13), expect: uint32(13)},
		{name: "ubigint", fieldType: common.TSDB_DATA_TYPE_UBIGINT, value: uint32(14), expect: uint64(14)},
		{name: "float", fieldType: common.TSDB_DATA_TYPE_FLOAT, value: float64(1.25), expect: float32(1.25)},
		{name: "double", fieldType: common.TSDB_DATA_TYPE_DOUBLE, value: float32(2.5), expect: float64(2.5)},
		{
			name:      "timestamp",
			fieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			value:     ts,
			precision: common.PrecisionMilliSecond,
			expect:    common.TimeToTimestamp(ts, common.PrecisionMilliSecond),
		},
		{name: "binary", fieldType: common.TSDB_DATA_TYPE_BINARY, value: "bin", expect: []byte("bin")},
		{name: "varbinary", fieldType: common.TSDB_DATA_TYPE_VARBINARY, value: []byte("var"), expect: []byte("var")},
		{name: "blob", fieldType: common.TSDB_DATA_TYPE_BLOB, value: types.TaosBlob([]byte("blob")), expect: []byte("blob")},
		{name: "geometry", fieldType: common.TSDB_DATA_TYPE_GEOMETRY, value: types.TaosGeometry([]byte{0x01, 0x02}), expect: []byte{0x01, 0x02}},
		{name: "decimal", fieldType: common.TSDB_DATA_TYPE_DECIMAL, value: []byte("12.3400"), expect: "12.3400"},
		{name: "decimal64", fieldType: common.TSDB_DATA_TYPE_DECIMAL64, value: types.TaosDecimal("56.7800"), expect: "56.7800"},
		{name: "nchar", fieldType: common.TSDB_DATA_TYPE_NCHAR, value: types.TaosNchar("nchar"), expect: "nchar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertInsertValue(tt.value, tt.fieldType, tt.precision)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertValueEqual(t, got, tt.expect)
		})
	}

	errTests := []struct {
		name      string
		fieldType int8
		value     driver.Value
		precision int
	}{
		{name: "bool_err", fieldType: common.TSDB_DATA_TYPE_BOOL, value: []int{1}},
		{name: "tinyint_err", fieldType: common.TSDB_DATA_TYPE_TINYINT, value: []int{1}},
		{name: "smallint_err", fieldType: common.TSDB_DATA_TYPE_SMALLINT, value: []int{1}},
		{name: "int_err", fieldType: common.TSDB_DATA_TYPE_INT, value: []int{1}},
		{name: "bigint_err", fieldType: common.TSDB_DATA_TYPE_BIGINT, value: []int{1}},
		{name: "utinyint_err", fieldType: common.TSDB_DATA_TYPE_UTINYINT, value: []int{1}},
		{name: "usmallint_err", fieldType: common.TSDB_DATA_TYPE_USMALLINT, value: []int{1}},
		{name: "uint_err", fieldType: common.TSDB_DATA_TYPE_UINT, value: []int{1}},
		{name: "ubigint_err", fieldType: common.TSDB_DATA_TYPE_UBIGINT, value: []int{1}},
		{name: "float_err", fieldType: common.TSDB_DATA_TYPE_FLOAT, value: []int{1}},
		{name: "double_err", fieldType: common.TSDB_DATA_TYPE_DOUBLE, value: []int{1}},
		{name: "timestamp_err", fieldType: common.TSDB_DATA_TYPE_TIMESTAMP, value: "not-a-timestamp", precision: common.PrecisionNanoSecond},
		{name: "binary_err", fieldType: common.TSDB_DATA_TYPE_BINARY, value: []int{1}},
		{name: "varbinary_err", fieldType: common.TSDB_DATA_TYPE_VARBINARY, value: []int{1}},
		{name: "blob_err", fieldType: common.TSDB_DATA_TYPE_BLOB, value: []int{1}},
		{name: "geometry_err", fieldType: common.TSDB_DATA_TYPE_GEOMETRY, value: []int{1}},
		{name: "decimal_err", fieldType: common.TSDB_DATA_TYPE_DECIMAL, value: []int{1}},
		{name: "nchar_err", fieldType: common.TSDB_DATA_TYPE_NCHAR, value: []int{1}},
		{name: "unsupported_field", fieldType: int8(127), value: int64(1)},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertInsertValue(tt.value, tt.fieldType, tt.precision)
			if err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestConvertQueryValue_AllTypesAndErrors(t *testing.T) {
	ts := time.Unix(1700000000, 123000000).UTC()
	tests := []struct {
		name   string
		value  driver.Value
		expect driver.Value
	}{
		{name: "time", value: ts, expect: []byte(ts.Format(time.RFC3339Nano))},
		{name: "taos_time", value: types.TaosTimestamp{T: ts, Precision: common.PrecisionNanoSecond}, expect: []byte(ts.Format(time.RFC3339Nano))},
		{name: "bool", value: true, expect: true},
		{name: "taos_bool", value: types.TaosBool(true), expect: true},
		{name: "float32", value: float32(1.25), expect: float64(1.25)},
		{name: "float64", value: float64(1.5), expect: float64(1.5)},
		{name: "taos_float", value: types.TaosFloat(1.75), expect: float64(1.75)},
		{name: "taos_double", value: types.TaosDouble(2.5), expect: float64(2.5)},
		{name: "int", value: int(3), expect: int64(3)},
		{name: "int8", value: int8(4), expect: int64(4)},
		{name: "int16", value: int16(5), expect: int64(5)},
		{name: "int32", value: int32(6), expect: int64(6)},
		{name: "int64", value: int64(7), expect: int64(7)},
		{name: "taos_tinyint", value: types.TaosTinyint(8), expect: int64(8)},
		{name: "taos_smallint", value: types.TaosSmallint(9), expect: int64(9)},
		{name: "taos_int", value: types.TaosInt(10), expect: int64(10)},
		{name: "taos_bigint", value: types.TaosBigint(11), expect: int64(11)},
		{name: "uint", value: uint(12), expect: uint64(12)},
		{name: "uint8", value: uint8(13), expect: uint64(13)},
		{name: "uint16", value: uint16(14), expect: uint64(14)},
		{name: "uint32", value: uint32(15), expect: uint64(15)},
		{name: "uint64", value: uint64(16), expect: uint64(16)},
		{name: "taos_utinyint", value: types.TaosUTinyint(17), expect: uint64(17)},
		{name: "taos_usmallint", value: types.TaosUSmallint(18), expect: uint64(18)},
		{name: "taos_uint", value: types.TaosUInt(19), expect: uint64(19)},
		{name: "taos_ubigint", value: types.TaosUBigint(20), expect: uint64(20)},
		{name: "string", value: "str", expect: []byte("str")},
		{name: "taos_nchar", value: types.TaosNchar("nchar"), expect: []byte("nchar")},
		{name: "taos_decimal", value: types.TaosDecimal("1.2300"), expect: []byte("1.2300")},
		{name: "bytes", value: []byte("bytes"), expect: []byte("bytes")},
		{name: "taos_binary", value: types.TaosBinary([]byte("bin")), expect: []byte("bin")},
		{name: "taos_varbinary", value: types.TaosVarBinary([]byte("var")), expect: []byte("var")},
		{name: "taos_geometry", value: types.TaosGeometry([]byte{0x01, 0x02}), expect: []byte{0x01, 0x02}},
		{name: "taos_json", value: types.TaosJson([]byte(`{"a":1}`)), expect: []byte(`{"a":1}`)},
		{name: "taos_blob", value: types.TaosBlob([]byte("blob")), expect: []byte("blob")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertQueryValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertValueEqual(t, got, tt.expect)
		})
	}

	if _, err := convertQueryValue(nil); err == nil {
		t.Fatalf("expected nil value error")
	}
	if _, err := convertQueryValue(struct{}{}); err == nil {
		t.Fatalf("expected unsupported type error")
	}
}

func TestHelperConversionFunctions_AllTypesAndErrors(t *testing.T) {
	ts := time.Unix(1700000000, 123456789).UTC()
	tsText := ts.Format(time.RFC3339Nano)

	t.Run("toBool", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect bool
		}{
			{true, true},
			{types.TaosBool(true), true},
			{float32(1), true},
			{float64(1), true},
			{types.TaosFloat(1), true},
			{types.TaosDouble(1), true},
			{int(1), true},
			{int8(1), true},
			{int16(1), true},
			{int32(1), true},
			{int64(1), true},
			{types.TaosTinyint(1), true},
			{types.TaosSmallint(1), true},
			{types.TaosInt(1), true},
			{types.TaosBigint(1), true},
			{uint(1), true},
			{uint8(1), true},
			{uint16(1), true},
			{uint32(1), true},
			{uint64(1), true},
			{types.TaosUTinyint(1), true},
			{types.TaosUSmallint(1), true},
			{types.TaosUInt(1), true},
			{types.TaosUBigint(1), true},
			{"true", true},
			{types.TaosNchar("true"), true},
		}
		for _, tc := range cases {
			got, err := toBool(tc.value)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected bool for %T: want %v got %v", tc.value, tc.expect, got)
			}
		}
		if _, err := toBool("bad-bool"); err == nil {
			t.Fatalf("expected bool parse error")
		}
		if _, err := toBool(types.TaosNchar("bad-bool")); err == nil {
			t.Fatalf("expected nchar bool parse error")
		}
		if _, err := toBool(struct{}{}); err == nil {
			t.Fatalf("expected unsupported bool type error")
		}
	})

	t.Run("toSignedInt", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect int64
		}{
			{true, 1},
			{false, 0},
			{types.TaosBool(true), 1},
			{types.TaosBool(false), 0},
			{float32(2), 2},
			{float64(3), 3},
			{types.TaosFloat(4), 4},
			{types.TaosDouble(5), 5},
			{int(6), 6},
			{int8(7), 7},
			{int16(8), 8},
			{int32(9), 9},
			{int64(10), 10},
			{types.TaosTinyint(11), 11},
			{types.TaosSmallint(12), 12},
			{types.TaosInt(13), 13},
			{types.TaosBigint(14), 14},
			{uint(15), 15},
			{uint8(16), 16},
			{uint16(17), 17},
			{uint32(18), 18},
			{uint64(19), 19},
			{types.TaosUTinyint(20), 20},
			{types.TaosUSmallint(21), 21},
			{types.TaosUInt(22), 22},
			{types.TaosUBigint(23), 23},
			{"24", 24},
			{types.TaosNchar("25"), 25},
		}
		for _, tc := range cases {
			got, err := toSignedInt(tc.value, 64)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected signed int for %T: want %d got %d", tc.value, tc.expect, got)
			}
		}
		if _, err := toSignedInt("x", 64); err == nil {
			t.Fatalf("expected signed int parse error")
		}
		if _, err := toSignedInt(types.TaosNchar("x"), 64); err == nil {
			t.Fatalf("expected nchar signed int parse error")
		}
		if _, err := toSignedInt(struct{}{}, 64); err == nil {
			t.Fatalf("expected unsupported signed int type error")
		}
	})

	t.Run("toUnsignedInt", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect uint64
		}{
			{true, 1},
			{false, 0},
			{types.TaosBool(true), 1},
			{types.TaosBool(false), 0},
			{float32(2), 2},
			{float64(3), 3},
			{types.TaosFloat(4), 4},
			{types.TaosDouble(5), 5},
			{int(6), 6},
			{int8(7), 7},
			{int16(8), 8},
			{int32(9), 9},
			{int64(10), 10},
			{types.TaosTinyint(11), 11},
			{types.TaosSmallint(12), 12},
			{types.TaosInt(13), 13},
			{types.TaosBigint(14), 14},
			{uint(15), 15},
			{uint8(16), 16},
			{uint16(17), 17},
			{uint32(18), 18},
			{uint64(19), 19},
			{types.TaosUTinyint(20), 20},
			{types.TaosUSmallint(21), 21},
			{types.TaosUInt(22), 22},
			{types.TaosUBigint(23), 23},
			{"24", 24},
			{types.TaosNchar("25"), 25},
		}
		for _, tc := range cases {
			got, err := toUnsignedInt(tc.value, 64)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected unsigned int for %T: want %d got %d", tc.value, tc.expect, got)
			}
		}
		if _, err := toUnsignedInt("-1", 64); err == nil {
			t.Fatalf("expected unsigned int parse error")
		}
		if _, err := toUnsignedInt(types.TaosNchar("-1"), 64); err == nil {
			t.Fatalf("expected nchar unsigned int parse error")
		}
		if _, err := toUnsignedInt(struct{}{}, 64); err == nil {
			t.Fatalf("expected unsupported unsigned int type error")
		}
	})

	t.Run("toFloat", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect float64
		}{
			{true, 1},
			{false, 0},
			{types.TaosBool(true), 1},
			{types.TaosBool(false), 0},
			{float32(2.5), 2.5},
			{float64(3.5), 3.5},
			{types.TaosFloat(4.5), 4.5},
			{types.TaosDouble(5.5), 5.5},
			{int(6), 6},
			{int8(7), 7},
			{int16(8), 8},
			{int32(9), 9},
			{int64(10), 10},
			{types.TaosTinyint(11), 11},
			{types.TaosSmallint(12), 12},
			{types.TaosInt(13), 13},
			{types.TaosBigint(14), 14},
			{uint(15), 15},
			{uint8(16), 16},
			{uint16(17), 17},
			{uint32(18), 18},
			{uint64(19), 19},
			{types.TaosUTinyint(20), 20},
			{types.TaosUSmallint(21), 21},
			{types.TaosUInt(22), 22},
			{types.TaosUBigint(23), 23},
			{"24.5", 24.5},
			{types.TaosNchar("25.5"), 25.5},
		}
		for _, tc := range cases {
			got, err := toFloat(tc.value, 64)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected float for %T: want %f got %f", tc.value, tc.expect, got)
			}
		}
		if _, err := toFloat("x", 64); err == nil {
			t.Fatalf("expected float parse error")
		}
		if _, err := toFloat(types.TaosNchar("x"), 64); err == nil {
			t.Fatalf("expected nchar float parse error")
		}
		if _, err := toFloat(struct{}{}, 64); err == nil {
			t.Fatalf("expected unsupported float type error")
		}
	})

	t.Run("toBytes", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect []byte
		}{
			{[]byte("a"), []byte("a")},
			{"b", []byte("b")},
			{types.TaosBinary([]byte("c")), []byte("c")},
			{types.TaosVarBinary([]byte("d")), []byte("d")},
			{types.TaosGeometry([]byte("e")), []byte("e")},
			{types.TaosJson([]byte("f")), []byte("f")},
			{types.TaosBlob([]byte("g")), []byte("g")},
			{types.TaosNchar("h"), []byte("h")},
		}
		for _, tc := range cases {
			got, err := toBytes(tc.value)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			assertValueEqual(t, got, tc.expect)
		}
		if _, err := toBytes(struct{}{}); err == nil {
			t.Fatalf("expected unsupported bytes type error")
		}
	})

	t.Run("toString", func(t *testing.T) {
		cases := []struct {
			value  interface{}
			expect string
		}{
			{"a", "a"},
			{types.TaosNchar("b"), "b"},
			{[]byte("c"), "c"},
			{types.TaosBinary([]byte("d")), "d"},
			{types.TaosVarBinary([]byte("e")), "e"},
			{types.TaosGeometry([]byte("f")), "f"},
			{types.TaosJson([]byte("g")), "g"},
			{types.TaosBlob([]byte("h")), "h"},
			{types.TaosDecimal("1.2300"), "1.2300"},
		}
		for _, tc := range cases {
			got, err := toString(tc.value)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected string for %T: want %s got %s", tc.value, tc.expect, got)
			}
		}
		if _, err := toString(struct{}{}); err == nil {
			t.Fatalf("expected unsupported string type error")
		}
	})

	t.Run("toTimestamp", func(t *testing.T) {
		cases := []struct {
			value     interface{}
			precision int
			expect    int64
		}{
			{ts, common.PrecisionNanoSecond, common.TimeToTimestamp(ts, common.PrecisionNanoSecond)},
			{types.TaosTimestamp{T: ts, Precision: common.PrecisionNanoSecond}, common.PrecisionMilliSecond, common.TimeToTimestamp(ts, common.PrecisionMilliSecond)},
			{float32(2), common.PrecisionNanoSecond, 2},
			{float64(3), common.PrecisionNanoSecond, 3},
			{types.TaosFloat(4), common.PrecisionNanoSecond, 4},
			{types.TaosDouble(5), common.PrecisionNanoSecond, 5},
			{int(6), common.PrecisionNanoSecond, 6},
			{int8(7), common.PrecisionNanoSecond, 7},
			{int16(8), common.PrecisionNanoSecond, 8},
			{int32(9), common.PrecisionNanoSecond, 9},
			{int64(10), common.PrecisionNanoSecond, 10},
			{types.TaosTinyint(11), common.PrecisionNanoSecond, 11},
			{types.TaosSmallint(12), common.PrecisionNanoSecond, 12},
			{types.TaosInt(13), common.PrecisionNanoSecond, 13},
			{types.TaosBigint(14), common.PrecisionNanoSecond, 14},
			{uint(15), common.PrecisionNanoSecond, 15},
			{uint8(16), common.PrecisionNanoSecond, 16},
			{uint16(17), common.PrecisionNanoSecond, 17},
			{uint32(18), common.PrecisionNanoSecond, 18},
			{uint64(19), common.PrecisionNanoSecond, 19},
			{types.TaosUTinyint(20), common.PrecisionNanoSecond, 20},
			{types.TaosUSmallint(21), common.PrecisionNanoSecond, 21},
			{types.TaosUInt(22), common.PrecisionNanoSecond, 22},
			{types.TaosUBigint(23), common.PrecisionNanoSecond, 23},
			{tsText, common.PrecisionMilliSecond, common.TimeToTimestamp(ts, common.PrecisionMilliSecond)},
			{types.TaosNchar(tsText), common.PrecisionMilliSecond, common.TimeToTimestamp(ts, common.PrecisionMilliSecond)},
		}
		for _, tc := range cases {
			got, err := toTimestamp(tc.value, tc.precision)
			if err != nil {
				t.Fatalf("unexpected error for %T: %v", tc.value, err)
			}
			if got != tc.expect {
				t.Fatalf("unexpected timestamp for %T: want %d got %d", tc.value, tc.expect, got)
			}
		}
		if _, err := toTimestamp("bad-time", common.PrecisionNanoSecond); err == nil {
			t.Fatalf("expected timestamp parse error")
		}
		if _, err := toTimestamp(types.TaosNchar("bad-time"), common.PrecisionNanoSecond); err == nil {
			t.Fatalf("expected nchar timestamp parse error")
		}
		if _, err := toTimestamp(struct{}{}, common.PrecisionNanoSecond); err == nil {
			t.Fatalf("expected unsupported timestamp type error")
		}
	})
}
