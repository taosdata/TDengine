//go:build go1.18
// +build go1.18

package stmt

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"hash/fnv"
	"math/rand"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

type fuzzUnsupportedType struct{}

var fuzzInsertDataTypes = []int8{
	common.TSDB_DATA_TYPE_BOOL,
	common.TSDB_DATA_TYPE_TINYINT,
	common.TSDB_DATA_TYPE_SMALLINT,
	common.TSDB_DATA_TYPE_INT,
	common.TSDB_DATA_TYPE_BIGINT,
	common.TSDB_DATA_TYPE_FLOAT,
	common.TSDB_DATA_TYPE_DOUBLE,
	common.TSDB_DATA_TYPE_TIMESTAMP,
	common.TSDB_DATA_TYPE_UTINYINT,
	common.TSDB_DATA_TYPE_USMALLINT,
	common.TSDB_DATA_TYPE_UINT,
	common.TSDB_DATA_TYPE_UBIGINT,
	common.TSDB_DATA_TYPE_BINARY,
	common.TSDB_DATA_TYPE_NCHAR,
	common.TSDB_DATA_TYPE_JSON,
	common.TSDB_DATA_TYPE_VARBINARY,
	common.TSDB_DATA_TYPE_GEOMETRY,
	common.TSDB_DATA_TYPE_BLOB,
}

func FuzzMarshalStmt2BinaryParity(f *testing.F) {
	f.Add([]byte("insert-basic"))
	f.Add([]byte("insert-all-null-fixed"))
	f.Add([]byte("insert-bool-with-nil"))
	f.Add([]byte("query-basic"))
	f.Add([]byte("query-time"))
	f.Add([]byte("tbname-empty"))

	f.Fuzz(func(t *testing.T, seed []byte) {
		r := newFuzzRand(seed)
		if r.Intn(2) == 0 {
			bindData, fields := buildInsertFuzzCase(r)
			assertMarshalParity(t, bindData, true, fields)
			return
		}
		bindData, fields := buildQueryFuzzCase(r)
		assertMarshalParity(t, bindData, false, fields)
	})
}

func assertMarshalParity(t *testing.T, bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField) {
	t.Helper()
	oldBuf, oldPanic, oldErr := marshalStmt2WithRecover(func() ([]byte, error) {
		return marshalStmt2BinaryLegacy(bindData, isInsert, fields)
	})
	newBuf, newPanic, newErr := marshalStmt2WithRecover(func() ([]byte, error) {
		return MarshalStmt2Binary(bindData, isInsert, fields)
	})
	if oldPanic != nil || newPanic != nil {
		t.Fatalf("panic mismatch old=%v new=%v isInsert=%v bindData=%#v fields=%#v", oldPanic, newPanic, isInsert, bindData, fields)
	}
	if (oldErr == nil) != (newErr == nil) {
		t.Fatalf("error mismatch old=%v new=%v isInsert=%v bindData=%#v fields=%#v", oldErr, newErr, isInsert, bindData, fields)
	}
	if oldErr == nil && !bytes.Equal(oldBuf, newBuf) {
		t.Fatalf("buffer mismatch isInsert=%v oldLen=%d newLen=%d bindData=%#v fields=%#v", isInsert, len(oldBuf), len(newBuf), bindData, fields)
	}
}

func marshalStmt2WithRecover(fn func() ([]byte, error)) (buffer []byte, panicValue interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicValue = r
		}
	}()
	buffer, err = fn()
	return
}

func newFuzzRand(seed []byte) *rand.Rand {
	h := fnv.New64a()
	_, _ = h.Write(seed)
	return rand.New(rand.NewSource(int64(h.Sum64())))
}

func buildInsertFuzzCase(r *rand.Rand) ([]*TaosStmt2BindData, []*Stmt2AllField) {
	if r.Intn(32) == 0 {
		return nil, nil
	}

	tableCount := 1 + r.Intn(3)
	if r.Intn(12) == 0 {
		tableCount = 0
	}
	colCount := r.Intn(4)
	tagCount := r.Intn(3)
	rows := 1 + r.Intn(4)
	if r.Intn(8) == 0 {
		rows = 0
	}
	needTBField := r.Intn(2) == 0

	fields := make([]*Stmt2AllField, 0, colCount+tagCount+1)
	if needTBField {
		fields = append(fields, &Stmt2AllField{
			Name:      "tb",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		})
	}

	colTypes := make([]int8, colCount)
	for i := 0; i < colCount; i++ {
		colTypes[i] = fuzzInsertDataTypes[r.Intn(len(fuzzInsertDataTypes))]
		fields = append(fields, &Stmt2AllField{
			Name:      fmt.Sprintf("c%d", i),
			FieldType: colTypes[i],
			Precision: common.PrecisionMilliSecond,
			BindType:  TAOS_FIELD_COL,
		})
	}

	tagTypes := make([]int8, tagCount)
	for i := 0; i < tagCount; i++ {
		tagTypes[i] = fuzzInsertDataTypes[r.Intn(len(fuzzInsertDataTypes))]
		fields = append(fields, &Stmt2AllField{
			Name:      fmt.Sprintf("t%d", i),
			FieldType: tagTypes[i],
			Precision: common.PrecisionMilliSecond,
			BindType:  TAOS_FIELD_TAG,
		})
	}

	bindData := make([]*TaosStmt2BindData, tableCount)
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		data := &TaosStmt2BindData{}
		if r.Intn(3) != 0 {
			data.TableName = fuzzASCIIString(r, 8)
		}
		if tagCount > 0 {
			data.Tags = make([]driver.Value, tagCount)
			for i := 0; i < tagCount; i++ {
				data.Tags[i] = fuzzInsertValue(r, tagTypes[i], true)
			}
		}
		if colCount > 0 {
			data.Cols = make([][]driver.Value, colCount)
			for i := 0; i < colCount; i++ {
				// Keep row counts consistent across columns; only vary nil vs empty when rows == 0.
				if rows == 0 && r.Intn(2) == 0 {
					data.Cols[i] = nil
					continue
				}
				col := make([]driver.Value, rows)
				for j := 0; j < rows; j++ {
					col[j] = fuzzInsertValue(r, colTypes[i], true)
				}
				data.Cols[i] = col
			}
		}
		bindData[tableIndex] = data
	}
	if r.Intn(24) == 0 {
		fields = nil
	}
	return bindData, fields
}

func buildQueryFuzzCase(r *rand.Rand) ([]*TaosStmt2BindData, []*Stmt2AllField) {
	if r.Intn(32) == 0 {
		return nil, nil
	}
	bindDataCount := 1
	if r.Intn(12) == 0 {
		bindDataCount = 2
	}
	bindData := make([]*TaosStmt2BindData, bindDataCount)
	for bi := 0; bi < bindDataCount; bi++ {
		data := &TaosStmt2BindData{}
		if r.Intn(10) == 0 {
			data.TableName = fuzzASCIIString(r, 8)
		}
		if r.Intn(10) == 0 {
			data.Tags = []driver.Value{fuzzInsertValue(r, common.TSDB_DATA_TYPE_INT, true)}
		}

		paramCount := 1 + r.Intn(4)
		if r.Intn(8) == 0 {
			paramCount = 0
		}
		if paramCount > 0 && r.Intn(10) != 0 {
			cols := make([][]driver.Value, paramCount)
			for i := 0; i < paramCount; i++ {
				rowCount := 1
				if r.Intn(12) == 0 {
					cols[i] = nil
					continue
				}
				col := make([]driver.Value, rowCount)
				for j := 0; j < rowCount; j++ {
					col[j] = fuzzQueryValue(r)
				}
				cols[i] = col
			}
			data.Cols = cols
		}
		bindData[bi] = data
	}

	var fields []*Stmt2AllField
	switch r.Intn(6) {
	case 0:
		fields = nil
	case 1:
		fields = []*Stmt2AllField{
			{Name: "tb", FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: TAOS_FIELD_TBNAME},
		}
	case 2:
		fields = []*Stmt2AllField{
			{Name: "q_col", FieldType: common.TSDB_DATA_TYPE_INT, BindType: TAOS_FIELD_COL},
		}
	case 3:
		fields = []*Stmt2AllField{
			{Name: "q_tag", FieldType: common.TSDB_DATA_TYPE_INT, BindType: TAOS_FIELD_TAG},
		}
	case 4:
		fields = []*Stmt2AllField{
			{Name: "tb", FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: TAOS_FIELD_TBNAME},
			{Name: "q_col", FieldType: common.TSDB_DATA_TYPE_INT, BindType: TAOS_FIELD_COL},
		}
	default:
		fields = []*Stmt2AllField{
			{Name: "tb", FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: TAOS_FIELD_TBNAME},
			{Name: "q_tag", FieldType: common.TSDB_DATA_TYPE_INT, BindType: TAOS_FIELD_TAG},
		}
	}
	return bindData, fields
}

func fuzzInsertValue(r *rand.Rand, fieldType int8, allowNil bool) driver.Value {
	if allowNil && r.Intn(5) == 0 {
		return nil
	}
	switch fieldType {
	case common.TSDB_DATA_TYPE_BOOL:
		return r.Intn(2) == 0
	case common.TSDB_DATA_TYPE_TINYINT:
		return int8(r.Intn(7) - 3)
	case common.TSDB_DATA_TYPE_SMALLINT:
		return int16(r.Intn(200) - 100)
	case common.TSDB_DATA_TYPE_INT:
		return int32(r.Intn(20000) - 10000)
	case common.TSDB_DATA_TYPE_BIGINT:
		return int64(r.Intn(200000) - 100000)
	case common.TSDB_DATA_TYPE_FLOAT:
		return float32(r.Intn(10000)-5000) / 100
	case common.TSDB_DATA_TYPE_DOUBLE:
		return float64(r.Intn(10000)-5000) / 100
	case common.TSDB_DATA_TYPE_TIMESTAMP:
		if r.Intn(2) == 0 {
			return int64(1700000000000 + r.Int63n(100000))
		}
		return time.Unix(1700000000+r.Int63n(100000), r.Int63n(1e9))
	case common.TSDB_DATA_TYPE_UTINYINT:
		return uint8(r.Intn(255))
	case common.TSDB_DATA_TYPE_USMALLINT:
		return uint16(r.Intn(65535))
	case common.TSDB_DATA_TYPE_UINT:
		return uint32(r.Uint32())
	case common.TSDB_DATA_TYPE_UBIGINT:
		return uint64(r.Uint32())
	case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR, common.TSDB_DATA_TYPE_JSON:
		if r.Intn(2) == 0 {
			return fuzzASCIIString(r, 16)
		}
		return fuzzBytes(r, 16)
	case common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_GEOMETRY, common.TSDB_DATA_TYPE_BLOB:
		if r.Intn(4) == 0 {
			return fuzzASCIIString(r, 16)
		}
		return fuzzBytes(r, 16)
	default:
		return nil
	}
}

func fuzzQueryValue(r *rand.Rand) driver.Value {
	// Keep most values in the supported set, and occasionally include unsupported
	// values to verify error-state parity.
	if r.Intn(8) == 0 {
		if r.Intn(2) == 0 {
			return nil
		}
		return fuzzUnsupportedType{}
	}
	switch r.Intn(14) {
	case 0:
		return fuzzASCIIString(r, 16)
	case 1:
		return fuzzBytes(r, 16)
	case 2:
		return int8(r.Intn(7) - 3)
	case 3:
		return int16(r.Intn(200) - 100)
	case 4:
		return int32(r.Intn(20000) - 10000)
	case 5:
		return int64(r.Intn(200000) - 100000)
	case 6:
		return uint8(r.Intn(255))
	case 7:
		return uint16(r.Intn(65535))
	case 8:
		return uint32(r.Uint32())
	case 9:
		return uint64(r.Uint32())
	case 10:
		return float32(r.Intn(10000)-5000) / 100
	case 11:
		return float64(r.Intn(10000)-5000) / 100
	case 12:
		return r.Intn(2) == 0
	default:
		return time.Unix(1700000000+r.Int63n(100000), r.Int63n(1e9))
	}
}

func fuzzASCIIString(r *rand.Rand, maxLen int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	n := r.Intn(maxLen + 1)
	if n == 0 {
		return ""
	}
	data := make([]byte, n)
	for i := 0; i < n; i++ {
		data[i] = letters[r.Intn(len(letters))]
	}
	return string(data)
}

func fuzzBytes(r *rand.Rand, maxLen int) []byte {
	n := r.Intn(maxLen + 1)
	data := make([]byte, n)
	for i := 0; i < n; i++ {
		data[i] = byte(r.Intn(256))
	}
	return data
}
