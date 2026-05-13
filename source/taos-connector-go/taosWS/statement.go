package taosWS

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

type Stmt struct {
	conn       *taosConn
	stmtHandle *unified.Stmt
	isInsert   bool
	cols       []*commonstmt.StmtField
}

func (stmt *Stmt) Close() error {
	if stmt.conn == nil || stmt.conn.isClosed() {
		return driver.ErrBadConn
	}
	if stmt.stmtHandle == nil {
		return driver.ErrBadConn
	}
	err := stmt.stmtHandle.Close(0)
	err = mapUnifiedConnError(err)
	stmt.stmtHandle = nil
	stmt.conn = nil
	return err
}

func (stmt *Stmt) NumInput() int {
	if stmt.cols != nil {
		return len(stmt.cols)
	}
	return -1
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.conn == nil || stmt.conn.isClosed() || stmt.stmtHandle == nil {
		return nil, driver.ErrBadConn
	}
	if stmt.isInsert && stmt.cols == nil {
		if err := stmt.ensureInsertColumnMeta(); err != nil {
			return nil, err
		}
	}
	if len(args) != len(stmt.cols) {
		return nil, fmt.Errorf("stmt exec error: wrong number of parameters")
	}
	if len(args) == 0 {
		return nil, unified.ErrStmtParamsEmpty
	}
	cols := make([][]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		cols[i] = []driver.Value{args[i]}
	}
	err := stmt.stmtHandle.Bind([]*commonstmt.TaosStmt2BindData{{
		Cols: cols,
	}})
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	affected, err := stmt.stmtHandle.Exec(0)
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	return driver.RowsAffected(affected), nil
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.conn == nil || stmt.conn.isClosed() || stmt.stmtHandle == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) == 0 {
		return nil, unified.ErrStmtParamsEmpty
	}
	cols := make([][]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		cols[i] = []driver.Value{args[i]}
	}
	err := stmt.stmtHandle.Bind([]*commonstmt.TaosStmt2BindData{{
		Cols: cols,
	}})
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	_, err = stmt.stmtHandle.Exec(0)
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	rs, err := stmt.stmtHandle.UseResult(0)
	if err != nil {
		return nil, mapUnifiedConnError(err)
	}
	return newRowsFromUnified(rs), nil
}

func (stmt *Stmt) ensureInsertColumnMeta() error {
	if stmt.stmtHandle == nil {
		return driver.ErrBadConn
	}
	cols, err := stmt.stmtHandle.ColFields()
	if err != nil {
		return mapUnifiedConnError(err)
	}
	stmt.cols = cols
	return nil
}

func (stmt *Stmt) CheckNamedValue(v *driver.NamedValue) error {
	if stmt.isInsert {
		if stmt.cols == nil {
			if err := stmt.ensureInsertColumnMeta(); err != nil {
				return err
			}
		}
		if v.Ordinal > len(stmt.cols) {
			return nil
		}
		fieldType := stmt.cols[v.Ordinal-1].FieldType
		converted, err := convertInsertValue(v.Value, fieldType, int(stmt.cols[v.Ordinal-1].Precision))
		if err != nil {
			return err
		}
		v.Value = converted
		return nil
	}
	converted, err := convertQueryValue(v.Value)
	if err != nil {
		return fmt.Errorf("CheckNamedValue: can not convert query value %v", v)
	}
	v.Value = converted
	return nil
}

func convertInsertValue(value driver.Value, fieldType int8, precision int) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}
	switch fieldType {
	case common.TSDB_DATA_TYPE_NULL:
		return nil, nil
	case common.TSDB_DATA_TYPE_BOOL:
		v, err := toBool(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to bool, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_TINYINT:
		v, err := toSignedInt(value, 8)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to tinyint, value type %T", value)
		}
		return int8(v), nil
	case common.TSDB_DATA_TYPE_SMALLINT:
		v, err := toSignedInt(value, 16)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to smallint, value type %T", value)
		}
		return int16(v), nil
	case common.TSDB_DATA_TYPE_INT:
		v, err := toSignedInt(value, 32)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to int, value type %T", value)
		}
		return int32(v), nil
	case common.TSDB_DATA_TYPE_BIGINT:
		v, err := toSignedInt(value, 64)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to bigint, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_UTINYINT:
		v, err := toUnsignedInt(value, 8)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to tinyint unsigned, value type %T", value)
		}
		return uint8(v), nil
	case common.TSDB_DATA_TYPE_USMALLINT:
		v, err := toUnsignedInt(value, 16)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to smallint unsigned, value type %T", value)
		}
		return uint16(v), nil
	case common.TSDB_DATA_TYPE_UINT:
		v, err := toUnsignedInt(value, 32)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to int unsigned, value type %T", value)
		}
		return uint32(v), nil
	case common.TSDB_DATA_TYPE_UBIGINT:
		v, err := toUnsignedInt(value, 64)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to bigint unsigned, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_FLOAT:
		v, err := toFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to float, value type %T", value)
		}
		return float32(v), nil
	case common.TSDB_DATA_TYPE_DOUBLE:
		v, err := toFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to double, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_TIMESTAMP:
		v, err := toTimestamp(value, precision)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to timestamp, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_BINARY:
		v, err := toBytes(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to binary, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_VARBINARY:
		v, err := toBytes(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to varbinary, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_BLOB:
		v, err := toBytes(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to blob, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_GEOMETRY:
		v, err := toBytes(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to geometry, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64:
		v, err := toString(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to decimal, value type %T", value)
		}
		return v, nil
	case common.TSDB_DATA_TYPE_NCHAR:
		v, err := toString(value)
		if err != nil {
			return nil, fmt.Errorf("CheckNamedValue: can not convert to nchar, value type %T", value)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("CheckNamedValue: unsupported field type %s", common.GetTypeName(int(fieldType)))
	}
}

func convertQueryValue(value driver.Value) (driver.Value, error) {
	switch v := value.(type) {
	case nil:
		return nil, errors.New("CheckNamedValue: value is nil")
	case time.Time:
		return []byte(v.Format(time.RFC3339Nano)), nil
	case types.TaosTimestamp:
		return []byte(v.T.Format(time.RFC3339Nano)), nil
	case bool:
		return v, nil
	case types.TaosBool:
		return bool(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case types.TaosFloat:
		return float64(v), nil
	case types.TaosDouble:
		return float64(v), nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case types.TaosTinyint:
		return int64(v), nil
	case types.TaosSmallint:
		return int64(v), nil
	case types.TaosInt:
		return int64(v), nil
	case types.TaosBigint:
		return int64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case types.TaosUTinyint:
		return uint64(v), nil
	case types.TaosUSmallint:
		return uint64(v), nil
	case types.TaosUInt:
		return uint64(v), nil
	case types.TaosUBigint:
		return uint64(v), nil
	case string:
		return []byte(v), nil
	case types.TaosNchar:
		return []byte(v), nil
	case types.TaosDecimal:
		return []byte(v), nil
	case []byte:
		return append([]byte(nil), v...), nil
	case types.TaosBinary:
		return append([]byte(nil), v...), nil
	case types.TaosVarBinary:
		return append([]byte(nil), v...), nil
	case types.TaosGeometry:
		return append([]byte(nil), v...), nil
	case types.TaosJson:
		return append([]byte(nil), v...), nil
	case types.TaosBlob:
		return append([]byte(nil), v...), nil
	default:
		return nil, fmt.Errorf("unsupported query value type %T", value)
	}
}

func toBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case types.TaosBool:
		return bool(v), nil
	case float32:
		return v > 0, nil
	case float64:
		return v > 0, nil
	case types.TaosFloat:
		return v > 0, nil
	case types.TaosDouble:
		return v > 0, nil
	case int:
		return v > 0, nil
	case int8:
		return v > 0, nil
	case int16:
		return v > 0, nil
	case int32:
		return v > 0, nil
	case int64:
		return v > 0, nil
	case types.TaosTinyint:
		return v > 0, nil
	case types.TaosSmallint:
		return v > 0, nil
	case types.TaosInt:
		return v > 0, nil
	case types.TaosBigint:
		return v > 0, nil
	case uint:
		return v > 0, nil
	case uint8:
		return v > 0, nil
	case uint16:
		return v > 0, nil
	case uint32:
		return v > 0, nil
	case uint64:
		return v > 0, nil
	case types.TaosUTinyint:
		return v > 0, nil
	case types.TaosUSmallint:
		return v > 0, nil
	case types.TaosUInt:
		return v > 0, nil
	case types.TaosUBigint:
		return v > 0, nil
	case string:
		return strconv.ParseBool(v)
	case types.TaosNchar:
		return strconv.ParseBool(string(v))
	default:
		return false, fmt.Errorf("unsupported bool type %T", value)
	}
}

func toSignedInt(value interface{}, bitSize int) (int64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case types.TaosBool:
		if v {
			return 1, nil
		}
		return 0, nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case types.TaosFloat:
		return int64(v), nil
	case types.TaosDouble:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case types.TaosTinyint:
		return int64(v), nil
	case types.TaosSmallint:
		return int64(v), nil
	case types.TaosInt:
		return int64(v), nil
	case types.TaosBigint:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case types.TaosUTinyint:
		return int64(v), nil
	case types.TaosUSmallint:
		return int64(v), nil
	case types.TaosUInt:
		return int64(v), nil
	case types.TaosUBigint:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 0, bitSize)
	case types.TaosNchar:
		return strconv.ParseInt(string(v), 0, bitSize)
	default:
		return 0, fmt.Errorf("unsupported signed integer type %T", value)
	}
}

func toUnsignedInt(value interface{}, bitSize int) (uint64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case types.TaosBool:
		if v {
			return 1, nil
		}
		return 0, nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case types.TaosFloat:
		return uint64(v), nil
	case types.TaosDouble:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case types.TaosTinyint:
		return uint64(v), nil
	case types.TaosSmallint:
		return uint64(v), nil
	case types.TaosInt:
		return uint64(v), nil
	case types.TaosBigint:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case types.TaosUTinyint:
		return uint64(v), nil
	case types.TaosUSmallint:
		return uint64(v), nil
	case types.TaosUInt:
		return uint64(v), nil
	case types.TaosUBigint:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 0, bitSize)
	case types.TaosNchar:
		return strconv.ParseUint(string(v), 0, bitSize)
	default:
		return 0, fmt.Errorf("unsupported unsigned integer type %T", value)
	}
}

func toFloat(value interface{}, bitSize int) (float64, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case types.TaosBool:
		if v {
			return 1, nil
		}
		return 0, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case types.TaosFloat:
		return float64(v), nil
	case types.TaosDouble:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case types.TaosTinyint:
		return float64(v), nil
	case types.TaosSmallint:
		return float64(v), nil
	case types.TaosInt:
		return float64(v), nil
	case types.TaosBigint:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case types.TaosUTinyint:
		return float64(v), nil
	case types.TaosUSmallint:
		return float64(v), nil
	case types.TaosUInt:
		return float64(v), nil
	case types.TaosUBigint:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, bitSize)
	case types.TaosNchar:
		return strconv.ParseFloat(string(v), bitSize)
	default:
		return 0, fmt.Errorf("unsupported float type %T", value)
	}
}

func toBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	case types.TaosBinary:
		return append([]byte(nil), v...), nil
	case types.TaosVarBinary:
		return append([]byte(nil), v...), nil
	case types.TaosGeometry:
		return append([]byte(nil), v...), nil
	case types.TaosJson:
		return append([]byte(nil), v...), nil
	case types.TaosBlob:
		return append([]byte(nil), v...), nil
	case types.TaosNchar:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported bytes type %T", value)
	}
}

func toString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case types.TaosNchar:
		return string(v), nil
	case []byte:
		return string(v), nil
	case types.TaosBinary:
		return string(v), nil
	case types.TaosVarBinary:
		return string(v), nil
	case types.TaosGeometry:
		return string(v), nil
	case types.TaosJson:
		return string(v), nil
	case types.TaosBlob:
		return string(v), nil
	case types.TaosDecimal:
		return string(v), nil
	default:
		return "", fmt.Errorf("unsupported string type %T", value)
	}
}

func toTimestamp(value interface{}, precision int) (int64, error) {
	switch v := value.(type) {
	case time.Time:
		return common.TimeToTimestamp(v, precision), nil
	case types.TaosTimestamp:
		return common.TimeToTimestamp(v.T, precision), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case types.TaosFloat:
		return int64(v), nil
	case types.TaosDouble:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case types.TaosTinyint:
		return int64(v), nil
	case types.TaosSmallint:
		return int64(v), nil
	case types.TaosInt:
		return int64(v), nil
	case types.TaosBigint:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case types.TaosUTinyint:
		return int64(v), nil
	case types.TaosUSmallint:
		return int64(v), nil
	case types.TaosUInt:
		return int64(v), nil
	case types.TaosUBigint:
		return int64(v), nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return 0, err
		}
		return common.TimeToTimestamp(t, precision), nil
	case types.TaosNchar:
		t, err := time.Parse(time.RFC3339Nano, string(v))
		if err != nil {
			return 0, err
		}
		return common.TimeToTimestamp(t, precision), nil
	default:
		return 0, fmt.Errorf("unsupported timestamp type %T", value)
	}
}
