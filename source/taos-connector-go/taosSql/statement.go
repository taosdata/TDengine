package taosSql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	stmtCommon "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/wrapper"
)

//Client can't get stmt structure even by reflection.
//So the sql can't contain unset table name and tags.

type Stmt struct {
	stmt     unsafe.Pointer
	tc       *taosConn
	pSql     string
	isInsert bool
	cols     []*stmtCommon.StmtField
}

func (stmt *Stmt) Close() error {
	if stmt.stmt != nil {
		locker.Lock()
		wrapper.TaosStmtClose(stmt.stmt)
		locker.Unlock()
		stmt.stmt = nil
	}
	return nil
}

func (stmt *Stmt) NumInput() int {
	if stmt.cols != nil {
		return len(stmt.cols)
	}
	return -1
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.tc == nil || stmt.tc.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != len(stmt.cols) {
		return nil, fmt.Errorf("stmt exec error: wrong number of parameters")
	}
	locker.Lock()
	defer locker.Unlock()
	code := wrapper.TaosStmtBindParam(stmt.stmt, args)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	code = wrapper.TaosStmtAddBatch(stmt.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	code = wrapper.TaosStmtExecute(stmt.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	affectRows := wrapper.TaosStmtAffectedRowsOnce(stmt.stmt)
	return driver.RowsAffected(affectRows), nil
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.tc == nil || stmt.tc.taos == nil {
		return nil, driver.ErrBadConn
	}
	locker.Lock()
	defer locker.Unlock()
	code := wrapper.TaosStmtBindParam(stmt.stmt, args)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	code = wrapper.TaosStmtAddBatch(stmt.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	code = wrapper.TaosStmtExecute(stmt.stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt.stmt)
		return nil, errors.NewError(code, errStr)
	}
	res := wrapper.TaosStmtUseResult(stmt.stmt)
	handler := asyncHandlerPool.Get()
	numFields := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	rs := newRows(handler, rowsHeader, res, precision, true, stmt.tc.timezone)
	return rs, nil
}

func (stmt *Stmt) CheckNamedValue(v *driver.NamedValue) error {
	if stmt.isInsert {
		if stmt.cols == nil {
			locker.Lock()
			code, num, fieldsP := wrapper.TaosStmtGetColFields(stmt.stmt)
			locker.Unlock()
			if code != 0 {
				errStr := wrapper.TaosStmtErrStr(stmt.stmt)
				return errors.NewError(code, errStr)
			}
			defer wrapper.TaosStmtReclaimFields(stmt.stmt, fieldsP)
			stmt.cols = wrapper.StmtParseFields(num, fieldsP)
		}
		if v.Ordinal > len(stmt.cols) {
			return nil
		}
		if v.Value == nil {
			return nil
		}
		switch stmt.cols[v.Ordinal-1].FieldType {
		case common.TSDB_DATA_TYPE_NULL:
			v.Value = nil
		case common.TSDB_DATA_TYPE_BOOL:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				v.Value = types.TaosBool(rv.Bool())
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosBool(rv.Float() > 0)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosBool(rv.Int() > 0)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosBool(rv.Uint() > 0)
			case reflect.String:
				vv, err := strconv.ParseBool(rv.String())
				if err != nil {
					return err
				}
				v.Value = types.TaosBool(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to bool", v)
			}
		case common.TSDB_DATA_TYPE_TINYINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosTinyint(1)
				} else {
					v.Value = types.TaosTinyint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosTinyint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosTinyint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosTinyint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseInt(rv.String(), 0, 8)
				if err != nil {
					return err
				}
				v.Value = types.TaosTinyint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to tinyint", v)
			}
		case common.TSDB_DATA_TYPE_SMALLINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosSmallint(1)
				} else {
					v.Value = types.TaosSmallint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosSmallint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosSmallint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosSmallint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseInt(rv.String(), 0, 16)
				if err != nil {
					return err
				}
				v.Value = types.TaosSmallint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to smallint", v)
			}
		case common.TSDB_DATA_TYPE_INT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosInt(1)
				} else {
					v.Value = types.TaosInt(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosInt(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosInt(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosInt(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseInt(rv.String(), 0, 32)
				if err != nil {
					return err
				}
				v.Value = types.TaosInt(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to int", v)
			}
		case common.TSDB_DATA_TYPE_BIGINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosBigint(1)
				} else {
					v.Value = types.TaosBigint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosBigint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosBigint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosBigint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseInt(rv.String(), 0, 64)
				if err != nil {
					return err
				}
				v.Value = types.TaosBigint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to bigint", v)
			}
		case common.TSDB_DATA_TYPE_FLOAT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosFloat(1)
				} else {
					v.Value = types.TaosFloat(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosFloat(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosFloat(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosFloat(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseFloat(rv.String(), 32)
				if err != nil {
					return err
				}
				v.Value = types.TaosFloat(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to float", v)
			}
		case common.TSDB_DATA_TYPE_DOUBLE:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosDouble(1)
				} else {
					v.Value = types.TaosDouble(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosDouble(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosDouble(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosDouble(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseFloat(rv.String(), 64)
				if err != nil {
					return err
				}
				v.Value = types.TaosDouble(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to double", v)
			}
		case common.TSDB_DATA_TYPE_BINARY:
			switch v.Value.(type) {
			case string:
				v.Value = types.TaosBinary(v.Value.(string))
			case []byte:
				v.Value = types.TaosBinary(v.Value.([]byte))
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to binary", v)
			}
		case common.TSDB_DATA_TYPE_VARBINARY:
			switch v.Value.(type) {
			case string:
				v.Value = types.TaosVarBinary(v.Value.(string))
			case []byte:
				v.Value = types.TaosVarBinary(v.Value.([]byte))
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to varbinary", v)
			}

		case common.TSDB_DATA_TYPE_GEOMETRY:
			switch v.Value.(type) {
			case string:
				v.Value = types.TaosGeometry(v.Value.(string))
			case []byte:
				v.Value = types.TaosGeometry(v.Value.([]byte))
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to geometry", v)
			}

		case common.TSDB_DATA_TYPE_TIMESTAMP:
			t, is := v.Value.(time.Time)
			if is {
				v.Value = types.TaosTimestamp{
					T:         t,
					Precision: int(stmt.cols[v.Ordinal-1].Precision),
				}
				return nil
			}
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Float32, reflect.Float64:
				t := common.TimestampConvertToTime(int64(rv.Float()), int(stmt.cols[v.Ordinal-1].Precision))
				v.Value = types.TaosTimestamp{
					T:         t,
					Precision: int(stmt.cols[v.Ordinal-1].Precision),
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				t := common.TimestampConvertToTime(rv.Int(), int(stmt.cols[v.Ordinal-1].Precision))
				v.Value = types.TaosTimestamp{
					T:         t,
					Precision: int(stmt.cols[v.Ordinal-1].Precision),
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				t := common.TimestampConvertToTime(int64(rv.Uint()), int(stmt.cols[v.Ordinal-1].Precision))
				v.Value = types.TaosTimestamp{
					T:         t,
					Precision: int(stmt.cols[v.Ordinal-1].Precision),
				}
			case reflect.String:
				t, err := time.Parse(time.RFC3339Nano, rv.String())
				if err != nil {
					return err
				}
				v.Value = types.TaosTimestamp{
					T:         t,
					Precision: int(stmt.cols[v.Ordinal-1].Precision),
				}
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to timestamp", v)
			}
		case common.TSDB_DATA_TYPE_NCHAR:
			switch v.Value.(type) {
			case string:
				v.Value = types.TaosNchar(v.Value.(string))
			case []byte:
				v.Value = types.TaosNchar(v.Value.([]byte))
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to nchar", v)
			}
		case common.TSDB_DATA_TYPE_UTINYINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosUTinyint(1)
				} else {
					v.Value = types.TaosUTinyint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosUTinyint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosUTinyint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosUTinyint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseUint(rv.String(), 0, 8)
				if err != nil {
					return err
				}
				v.Value = types.TaosUTinyint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to tinyint unsigned", v)
			}
		case common.TSDB_DATA_TYPE_USMALLINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosUSmallint(1)
				} else {
					v.Value = types.TaosUSmallint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosUSmallint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosUSmallint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosUSmallint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseUint(rv.String(), 0, 16)
				if err != nil {
					return err
				}
				v.Value = types.TaosUSmallint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to smallint unsigned", v)
			}
		case common.TSDB_DATA_TYPE_UINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosUInt(1)
				} else {
					v.Value = types.TaosUInt(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosUInt(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosUInt(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosUInt(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseUint(rv.String(), 0, 32)
				if err != nil {
					return err
				}
				v.Value = types.TaosUInt(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to int unsigned", v)
			}
		case common.TSDB_DATA_TYPE_UBIGINT:
			rv := reflect.ValueOf(v.Value)
			switch rv.Kind() {
			case reflect.Bool:
				if rv.Bool() {
					v.Value = types.TaosUBigint(1)
				} else {
					v.Value = types.TaosUBigint(0)
				}
			case reflect.Float32, reflect.Float64:
				v.Value = types.TaosUBigint(rv.Float())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.Value = types.TaosUBigint(rv.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v.Value = types.TaosUBigint(rv.Uint())
			case reflect.String:
				vv, err := strconv.ParseUint(rv.String(), 0, 64)
				if err != nil {
					return err
				}
				v.Value = types.TaosUBigint(vv)
			default:
				return fmt.Errorf("CheckNamedValue:%v can not convert to bigint unsigned", v)
			}
		default:
			return fmt.Errorf("CheckNamedValue: unsupported field type %s", common.GetTypeName(int(stmt.cols[v.Ordinal-1].FieldType)))
		}
		return nil
	}
	if v.Value == nil {
		return nil
	}
	t, is := v.Value.(time.Time)
	if is {
		v.Value = types.TaosBinary(t.Format(time.RFC3339Nano))
		return nil
	}
	rv := reflect.ValueOf(v.Value)
	switch rv.Kind() {
	case reflect.Bool:
		v.Value = types.TaosBool(rv.Bool())
	case reflect.Float32, reflect.Float64:
		v.Value = types.TaosDouble(rv.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.Value = types.TaosBigint(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.Value = types.TaosUBigint(rv.Uint())
	case reflect.String:
		v.Value = types.TaosBinary(rv.String())
	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek == reflect.Uint8 {
			v.Value = types.TaosBinary(rv.Bytes())
		} else {
			return fmt.Errorf("CheckNamedValue: can not convert query value %v", v)
		}
	default:
		return fmt.Errorf("CheckNamedValue: can not convert query value %v", v)
	}
	return nil
}
