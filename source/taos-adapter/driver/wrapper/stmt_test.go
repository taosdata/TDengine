package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/param"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	taosError "github.com/taosdata/taosadapter/v3/driver/errors"
	taosTypes "github.com/taosdata/taosadapter/v3/driver/types"
)

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test stmt with taos_stmt_bind_param_batch
func TestStmt(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_wrapper")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_wrapper precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_wrapper"))

	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	for i, tc := range []struct {
		tbType      string
		pos         string
		params      [][]driver.Value
		bindType    []*taosTypes.ColumnType
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, v int",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosIntType}},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, v bool",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBool(true)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBoolType}},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, v tinyint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosTinyintType}},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, v smallint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosSmallintType}},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, v bigint",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosBigintType}},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, v tinyint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUTinyint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUTinyintType}},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, v smallint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUSmallint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUSmallintType}},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, v int unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUInt(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUIntType}},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, v bigint unsigned",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosUBigint(1)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosUBigintType}},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, v float",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosFloat(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosFloatType}},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, v double",
			pos:         "?, ?",
			params:      [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosDouble(1.2)}},
			bindType:    []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {Type: taosTypes.TaosDoubleType}},
			expectValue: 1.2,
		},
		{
			tbType: "ts timestamp, v binary(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosBinary("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosBinaryType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, v varbinary(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosVarBinary("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosVarBinaryType,
				MaxLen: 3,
			}},
			expectValue: []byte("yes"),
		}, //3
		{
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosGeometry{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosGeometryType,
				MaxLen: 100,
			}},
			expectValue: []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
		}, //3
		{
			tbType: "ts timestamp, v nchar(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {taosTypes.TaosNchar("yes")}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosNcharType,
				MaxLen: 3,
			}},
			expectValue: "yes",
		}, //3
		{
			tbType: "ts timestamp, v nchar(8)",
			pos:    "?, ?",
			params: [][]driver.Value{{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}}, {nil}},
			bindType: []*taosTypes.ColumnType{{Type: taosTypes.TaosTimestampType}, {
				Type:   taosTypes.TaosNcharType,
				MaxLen: 1,
			}},
			expectValue: nil,
		}, //1
	} {
		tbName := fmt.Sprintf("test_fast_insert_%02d", i)
		tbType := tc.tbType
		drop := fmt.Sprintf("drop table if exists %s", tbName)
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := TaosStmtInit(conn)
			code := TaosStmtPrepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			isInsert, code := TaosStmtIsInsert(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			if !isInsert {
				t.Errorf("expect insert stmt")
				return
			}
			code = TaosStmtBindParamBatch(insertStmt, tc.params, tc.bindType)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtClose(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select v from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			if len(result) != 1 {
				t.Errorf("expect %d got %d", 1, len(result))
				return
			}
			assert.Equal(t, tc.expectValue, result[0][0])
		})
	}

}

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test stmt insert with taos_stmt_bind_param
func TestStmtExec(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_wrapper")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_wrapper"))
	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	for i, tc := range []struct {
		tbType      string
		pos         string
		params      []driver.Value
		expectValue interface{}
	}{
		{
			tbType:      "ts timestamp, v int",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosInt(1)},
			expectValue: int32(1),
		},
		{
			tbType:      "ts timestamp, v bool",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBool(true)},
			expectValue: true,
		},
		{
			tbType:      "ts timestamp, v tinyint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosTinyint(1)},
			expectValue: int8(1),
		},
		{
			tbType:      "ts timestamp, v smallint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosSmallint(1)},
			expectValue: int16(1),
		},
		{
			tbType:      "ts timestamp, v bigint",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBigint(1)},
			expectValue: int64(1),
		},
		{
			tbType:      "ts timestamp, v tinyint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUTinyint(1)},
			expectValue: uint8(1),
		},
		{
			tbType:      "ts timestamp, v smallint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUSmallint(1)},
			expectValue: uint16(1),
		},
		{
			tbType:      "ts timestamp, v int unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUInt(1)},
			expectValue: uint32(1),
		},
		{
			tbType:      "ts timestamp, v bigint unsigned",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosUBigint(1)},
			expectValue: uint64(1),
		},
		{
			tbType:      "ts timestamp, v float",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosFloat(1.2)},
			expectValue: float32(1.2),
		},
		{
			tbType:      "ts timestamp, v double",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosDouble(1.2)},
			expectValue: 1.2,
		},
		{
			tbType:      "ts timestamp, v binary(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosBinary("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, v varbinary(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosVarBinary("yes")},
			expectValue: []byte("yes"),
		}, //3
		{
			tbType:      "ts timestamp, v geometry(100)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosGeometry{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			expectValue: []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
		}, //3
		{
			tbType:      "ts timestamp, v nchar(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, taosTypes.TaosNchar("yes")},
			expectValue: "yes",
		}, //3
		{
			tbType:      "ts timestamp, v nchar(8)",
			pos:         "?, ?",
			params:      []driver.Value{taosTypes.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}, nil},
			expectValue: nil,
		}, //1
	} {
		tbName := fmt.Sprintf("test_fast_insert_2_%02d", i)
		tbType := tc.tbType
		drop := fmt.Sprintf("drop table if exists %s", tbName)
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		pos := tc.pos
		sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
		var err error
		t.Run(name, func(t *testing.T) {
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			insertStmt := TaosStmtInit(conn)
			code := TaosStmtPrepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtBindParam(insertStmt, tc.params)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtAddBatch(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			code = TaosStmtExecute(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			affectedRows := TaosStmtAffectedRowsOnce(insertStmt)
			if affectedRows != 1 {
				t.Errorf("expect 1 got %d", affectedRows)
				return
			}
			code = TaosStmtClose(insertStmt)
			if code != 0 {
				errStr := TaosStmtErrStr(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select v from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			if len(result) != 1 {
				t.Errorf("expect %d got %d", 1, len(result))
				return
			}
			assert.Equal(t, tc.expectValue, result[0][0])
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test stmt query
func TestStmtQuery(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_wrapper"))

	err = exec(conn, "use test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	for i, tc := range []struct {
		tbType string
		data   string
		clause string
		params *param.Param
		skip   bool
	}{
		{
			tbType: "ts timestamp, v int",
			data:   "0, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddInt(1),
		},
		{
			tbType: "ts timestamp, v bool",
			data:   "now, true",
			clause: "v = ?",
			params: param.NewParam(1).AddBool(true),
		},
		{
			tbType: "ts timestamp, v tinyint",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddTinyint(3),
		},
		{
			tbType: "ts timestamp, v smallint",
			data:   "now, 5",
			clause: "v = ?",
			params: param.NewParam(1).AddSmallint(5),
		},
		{
			tbType: "ts timestamp, v int",
			data:   "now, 6",
			clause: "v = ?",
			params: param.NewParam(1).AddInt(6),
		},
		{
			tbType: "ts timestamp, v bigint",
			data:   "now, 7",
			clause: "v = ?",
			params: param.NewParam(1).AddBigint(7),
		},
		{
			tbType: "ts timestamp, v tinyint unsigned",
			data:   "now, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddUTinyint(1),
		},
		{
			tbType: "ts timestamp, v smallint unsigned",
			data:   "now, 2",
			clause: "v = ?",
			params: param.NewParam(1).AddUSmallint(2),
		},
		{
			tbType: "ts timestamp, v int unsigned",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddUInt(3),
		},
		{
			tbType: "ts timestamp, v bigint unsigned",
			data:   "now, 4",
			clause: "v = ?",
			params: param.NewParam(1).AddUBigint(4),
		},
		{
			tbType: "ts timestamp, v tinyint unsigned",
			data:   "now, 1",
			clause: "v = ?",
			params: param.NewParam(1).AddUTinyint(1),
		},
		{
			tbType: "ts timestamp, v smallint unsigned",
			data:   "now, 2",
			clause: "v = ?",
			params: param.NewParam(1).AddUSmallint(2),
		},
		{
			tbType: "ts timestamp, v int unsigned",
			data:   "now, 3",
			clause: "v = ?",
			params: param.NewParam(1).AddUInt(3),
		},
		{
			tbType: "ts timestamp, v bigint unsigned",
			data:   "now, 4",
			clause: "v = ?",
			params: param.NewParam(1).AddUBigint(4),
		},
		{
			tbType: "ts timestamp, v float",
			data:   "now, 1.2",
			clause: "v = ?",
			params: param.NewParam(1).AddFloat(1.2),
		},
		{
			tbType: "ts timestamp, v double",
			data:   "now, 1.3",
			clause: "v = ?",
			params: param.NewParam(1).AddDouble(1.3),
		},
		{
			tbType: "ts timestamp, v double",
			data:   "now, 1.4",
			clause: "v = ?",
			params: param.NewParam(1).AddDouble(1.4),
		},
		{
			tbType: "ts timestamp, v binary(8)",
			data:   "now, 'yes'",
			clause: "v = ?",
			params: param.NewParam(1).AddBinary([]byte("yes")),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "now, 'OK'",
			clause: "v = ?",
			params: param.NewParam(1).AddNchar("OK"),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "1622282105000000, 'NOW'",
			clause: "ts = ? and v = ?",
			params: param.NewParam(2).AddTimestamp(time.Unix(1622282105, 0), common.PrecisionMicroSecond).AddBinary([]byte("NOW")),
		},
		{
			tbType: "ts timestamp, v nchar(8)",
			data:   "1622282105000000, 'NOW'",
			clause: "ts = ? and v = ?",
			params: param.NewParam(2).AddBigint(1622282105000000).AddBinary([]byte("NOW")),
		},
	} {
		tbName := fmt.Sprintf("test_stmt_query%02d", i)
		tbType := tc.tbType
		create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
		insert := fmt.Sprintf("insert into %s values(%s)", tbName, tc.data)
		params := tc.params
		sql := fmt.Sprintf("select * from %s where %s", tbName, tc.clause)
		name := fmt.Sprintf("%02d-%s", i, tbType)
		var err error
		t.Run(name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skip, not support yet")
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, insert); err != nil {
				t.Error(err)
				return
			}

			rows, err := StmtQuery(t, conn, sql, params)
			if err != nil {
				t.Error(err)
				return
			}
			t.Log(rows)
		})
	}
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := TaosQuery(conn, sql)
	defer TaosFreeResult(res)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		return nil, taosError.NewError(code, errStr)
	}
	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			return nil, taosError.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

func StmtQuery(t *testing.T, conn unsafe.Pointer, sql string, params *param.Param) (rows [][]driver.Value, err error) {
	stmt := TaosStmtInit(conn)
	if stmt == nil {
		err = taosError.NewError(0xffff, "failed to init stmt")
		return
	}
	defer TaosStmtClose(stmt)
	code := TaosStmtPrepare(stmt, sql)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	value := params.GetValues()
	code = TaosStmtBindParam(stmt, value)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		return nil, taosError.NewError(code, errStr)
	}
	res := TaosStmtUseResult(stmt)
	numFields := TaosFieldCount(res)
	rowsHeader, err := ReadColumn(res, numFields)
	t.Log(rowsHeader)
	if err != nil {
		return nil, err
	}
	precision := TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(taosError.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := taosError.NewError(code, errStr)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		d, err := parser.ReadBlock(block, blockSize, rowsHeader.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		data = append(data, d...)
	}
	return data, nil
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test get field
func TestGetFields(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	stmt := TaosStmtInit(conn)
	defer func() {
		TaosStmtClose(stmt)
		err = exec(conn, "drop database if exists test_stmt_field")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt_field")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt_field"))

	err = exec(conn, "create table if not exists test_stmt_field.all_type(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20)"+
		")")
	if err != nil {
		t.Error(err)
		return
	}
	code := TaosStmtPrepare(stmt, "insert into ? using test_stmt_field.all_type tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code = TaosStmtSetTBName(stmt, "test_stmt_field.ct2")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, tagCount, tagsP := TaosStmtGetTagFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmtReclaimFields(stmt, tagsP)
	code, columnCount, columnsP := TaosStmtGetColFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmtReclaimFields(stmt, columnsP)
	columns := StmtParseFields(columnCount, columnsP)
	tags := StmtParseFields(tagCount, tagsP)
	assert.Equal(t, []*stmtCommon.StmtField{
		{Name: "ts", FieldType: 9, Bytes: 8},
		{Name: "c1", FieldType: 1, Bytes: 1},
		{Name: "c2", FieldType: 2, Bytes: 1},
		{Name: "c3", FieldType: 3, Bytes: 2},
		{Name: "c4", FieldType: 4, Bytes: 4},
		{Name: "c5", FieldType: 5, Bytes: 8},
		{Name: "c6", FieldType: 11, Bytes: 1},
		{Name: "c7", FieldType: 12, Bytes: 2},
		{Name: "c8", FieldType: 13, Bytes: 4},
		{Name: "c9", FieldType: 14, Bytes: 8},
		{Name: "c10", FieldType: 6, Bytes: 4},
		{Name: "c11", FieldType: 7, Bytes: 8},
		{Name: "c12", FieldType: 8, Bytes: 22},
		{Name: "c13", FieldType: 10, Bytes: 82},
	}, columns)
	assert.Equal(t, []*stmtCommon.StmtField{
		{Name: "tts", FieldType: 9, Bytes: 8},
		{Name: "tc1", FieldType: 1, Bytes: 1},
		{Name: "tc2", FieldType: 2, Bytes: 1},
		{Name: "tc3", FieldType: 3, Bytes: 2},
		{Name: "tc4", FieldType: 4, Bytes: 4},
		{Name: "tc5", FieldType: 5, Bytes: 8},
		{Name: "tc6", FieldType: 11, Bytes: 1},
		{Name: "tc7", FieldType: 12, Bytes: 2},
		{Name: "tc8", FieldType: 13, Bytes: 4},
		{Name: "tc9", FieldType: 14, Bytes: 8},
		{Name: "tc10", FieldType: 6, Bytes: 4},
		{Name: "tc11", FieldType: 7, Bytes: 8},
		{Name: "tc12", FieldType: 8, Bytes: 22},
		{Name: "tc13", FieldType: 10, Bytes: 82},
	}, tags)
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test get fields with common table
func TestGetFieldsCommonTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	stmt := TaosStmtInit(conn)
	defer func() {
		TaosStmtClose(stmt)
		err = exec(conn, "drop database if exists test_stmt_field")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt_field")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt_field"))

	TaosSelectDB(conn, "test_stmt_field")
	err = exec(conn, "create table if not exists ct(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")")
	if err != nil {
		t.Error(err)
		return
	}
	code := TaosStmtPrepare(stmt, "insert into ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, num, _ := TaosStmtGetTagFields(stmt)
	assert.NotEqual(t, 0, code)
	assert.Equal(t, 0, num)
	code, columnCount, columnsP := TaosStmtGetColFields(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmtReclaimFields(stmt, columnsP)
	columns := StmtParseFields(columnCount, columnsP)
	assert.Equal(t, []*stmtCommon.StmtField{
		{Name: "ts", FieldType: 9, Bytes: 8},
		{Name: "c1", FieldType: 1, Bytes: 1},
		{Name: "c2", FieldType: 2, Bytes: 1},
		{Name: "c3", FieldType: 3, Bytes: 2},
		{Name: "c4", FieldType: 4, Bytes: 4},
		{Name: "c5", FieldType: 5, Bytes: 8},
		{Name: "c6", FieldType: 11, Bytes: 1},
		{Name: "c7", FieldType: 12, Bytes: 2},
		{Name: "c8", FieldType: 13, Bytes: 4},
		{Name: "c9", FieldType: 14, Bytes: 8},
		{Name: "c10", FieldType: 6, Bytes: 4},
		{Name: "c11", FieldType: 7, Bytes: 8},
		{Name: "c12", FieldType: 8, Bytes: 22},
		{Name: "c13", FieldType: 10, Bytes: 82},
	}, columns)
}

func exec(conn unsafe.Pointer, sql string) error {
	res := TaosQuery(conn, sql)
	if code := TaosError(res); code != 0 {
		if (code & 0xffff) == 0x3d3 {
			//Conflict transaction not completed, retry in 100ms
			TaosFreeResult(res)
			time.Sleep(100 * time.Millisecond)
			return exec(conn, sql)
		}
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		return taosError.NewError(code, errStr)
	}
	TaosFreeResult(res)
	return nil
}

// @author: xftan
// @date: 2023/10/13 11:31
// @description: test stmt set tags
func TestTaosStmtSetTags(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_wrapper")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create database if not exists test_wrapper precision 'us' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_wrapper"))

	defer func() {
		err = exec(conn, "drop database if exists test_wrapper")
		if err != nil {
			t.Error(err)
		}
	}()
	err = exec(conn, "create table if not exists test_wrapper.tgs(ts timestamp,v int) tags (tts timestamp,"+
		"t1 bool,"+
		"t2 tinyint,"+
		"t3 smallint,"+
		"t4 int,"+
		"t5 bigint,"+
		"t6 tinyint unsigned,"+
		"t7 smallint unsigned,"+
		"t8 int unsigned,"+
		"t9 bigint unsigned,"+
		"t10 float,"+
		"t11 double,"+
		"t12 binary(20),"+
		"t13 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_wrapper.json_tag (ts timestamp,v int) tags (info json)")
	if err != nil {
		t.Error(err)
		return
	}
	stmt := TaosStmtInit(conn)
	if stmt == nil {
		err = taosError.NewError(0xffff, "failed to init stmt")
		t.Error(err)
		return
	}
	//defer TaosStmtClose(stmt)
	code := TaosStmtPrepare(stmt, "insert into ? using test_wrapper.tgs tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}

	code = TaosStmtSetTBName(stmt, "test_wrapper.t0")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	now := time.Now()
	code = TaosStmtSetTags(stmt, param.NewParam(14).
		AddTimestamp(now, common.PrecisionMicroSecond).
		AddBool(true).
		AddTinyint(2).
		AddSmallint(3).
		AddInt(4).
		AddBigint(5).
		AddUTinyint(6).
		AddUSmallint(7).
		AddUInt(8).
		AddUBigint(9).
		AddFloat(10).
		AddDouble(11).
		AddBinary([]byte("binary")).
		AddNchar("nchar").
		GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(100).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetSubTBName(stmt, "test_wrapper.t1")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTags(stmt, param.NewParam(14).
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		AddNull().
		GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(101).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}

	code = TaosStmtPrepare(stmt, "insert into ? using test_wrapper.json_tag tags(?) values (?,?)")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTBName(stmt, "test_wrapper.t2")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtSetTags(stmt, param.NewParam(1).AddJson([]byte(`{"a":"b"}`)).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtBindParam(stmt, param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(102).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	code = TaosStmtClose(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		t.Error(taosError.NewError(code, errStr))
		return
	}
	data, err := query(conn, "select tbname,tgs.* from test_wrapper.tgs where v >= 100")
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, 2, len(data))
	for i := 0; i < 2; i++ {

		switch data[i][0] {
		case "t0":
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][1].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, int32(100), data[i][2].(int32))
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][3].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, true, data[i][4].(bool))
			assert.Equal(t, int8(2), data[i][5].(int8))
			assert.Equal(t, int16(3), data[i][6].(int16))
			assert.Equal(t, int32(4), data[i][7].(int32))
			assert.Equal(t, int64(5), data[i][8].(int64))
			assert.Equal(t, uint8(6), data[i][9].(uint8))
			assert.Equal(t, uint16(7), data[i][10].(uint16))
			assert.Equal(t, uint32(8), data[i][11].(uint32))
			assert.Equal(t, uint64(9), data[i][12].(uint64))
			assert.Equal(t, float32(10), data[i][13].(float32))
			assert.Equal(t, float64(11), data[i][14].(float64))
			assert.Equal(t, "binary", data[i][15].(string))
			assert.Equal(t, "nchar", data[i][16].(string))
		case "t1":
			assert.Equal(t, now.UTC().UnixNano()/1e3, data[i][1].(time.Time).UTC().UnixNano()/1e3)
			assert.Equal(t, int32(101), data[i][2].(int32))
			for j := 0; j < 14; j++ {
				assert.Nil(t, data[i][3+j])
			}
		}
	}

	data, err = query(conn, "select tbname,json_tag.* from test_wrapper.json_tag where v >= 100")
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, 1, len(data))
	assert.Equal(t, "t2", data[0][0].(string))
	assert.Equal(t, now.UTC().UnixNano()/1e3, data[0][1].(time.Time).UTC().UnixNano()/1e3)
	assert.Equal(t, int32(102), data[0][2].(int32))
	assert.Equal(t, []byte(`{"a":"b"}`), data[0][3].([]byte))
}

func TestTaosStmtGetParam(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	require.NoError(t, err)
	defer TaosClose(conn)

	err = exec(conn, "drop database if exists test_stmt_get_param")
	require.NoError(t, err)
	err = exec(conn, "create database if not exists test_stmt_get_param")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_stmt_get_param"))

	defer func() {
		err = exec(conn, "drop database if exists test_stmt_get_param")
		assert.NoError(t, err)
	}()

	err = exec(conn,
		"create table if not exists test_stmt_get_param.stb(ts TIMESTAMP,current float,voltage int,phase float) TAGS (groupid int,location varchar(24))")
	require.NoError(t, err)

	stmt := TaosStmtInit(conn)
	require.NotNilf(t, stmt, "failed to init stmt")
	defer TaosStmtClose(stmt)

	code := TaosStmtPrepare(stmt, "insert into test_stmt_get_param.tb_0 using test_stmt_get_param.stb tags(?,?) values (?,?,?,?)")
	require.Equal(t, 0, code, TaosStmtErrStr(stmt))

	dt, dl, err := TaosStmtGetParam(stmt, 0) // ts
	require.NoError(t, err)
	assert.Equal(t, 9, dt)
	assert.Equal(t, 8, dl)

	dt, dl, err = TaosStmtGetParam(stmt, 1) // current
	require.NoError(t, err)
	assert.Equal(t, 6, dt)
	assert.Equal(t, 4, dl)

	dt, dl, err = TaosStmtGetParam(stmt, 2) // voltage
	require.NoError(t, err)
	assert.Equal(t, 4, dt)
	assert.Equal(t, 4, dl)

	dt, dl, err = TaosStmtGetParam(stmt, 3) // phase
	require.NoError(t, err)
	assert.Equal(t, 6, dt)
	assert.Equal(t, 4, dl)

	_, _, err = TaosStmtGetParam(stmt, 4) // invalid index
	require.Error(t, err)
}

func TestStmtJson(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt_json")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt_json precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt_json"))
	err = exec(conn, "use test_stmt_json")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table test_json_stb(ts timestamp, v int) tags (t json)")
	if err != nil {
		t.Error(err)
		return
	}
	stmt := TaosStmtInitWithReqID(conn, 0xbb123)
	defer func() {
		code := TaosStmtClose(stmt)
		if code != 0 {
			errStr := TaosStmtErrStr(stmt)
			err = taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
	}()
	prepareInsertSql := "insert into ? using test_json_stb tags(?) values (?,?)"
	code := TaosStmtPrepare(stmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code = TaosStmtSetTBNameTags(stmt, "ctb1", param.NewParam(1).AddJson([]byte(`{"a":1,"b":"xx"}`)).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	args := param.NewParam(2).AddTimestamp(now, common.PrecisionMilliSecond).AddInt(1).GetValues()
	code = TaosStmtBindParam(stmt, args)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}

	code = TaosStmtAddBatch(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	affected := TaosStmtAffectedRowsOnce(stmt)
	assert.Equal(t, 1, affected)

	code = TaosStmtPrepare(stmt, "select * from test_json_stb where t->'a' = ?")
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	count, code := TaosStmtNumParams(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 1, count)
	code = TaosStmtBindParam(stmt, param.NewParam(1).AddBigint(1).GetValues())
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code = TaosStmtExecute(stmt)
	if code != 0 {
		errStr := TaosStmtErrStr(stmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	res := TaosStmtUseResult(stmt)

	fileCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	t.Log(result)
}

func ensureDBCreated(conn unsafe.Pointer, dbName string) error {
	for i := 0; i < 100; i++ {
		value, err := query(conn, fmt.Sprintf(`select * from performance_schema.perf_trans where db = '%s'`, dbName))
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err = query(conn, fmt.Sprintf(`select * from information_schema.ins_databases where name = '%s'`, dbName))
			if err != nil {
				return err
			}
			if len(value) > 0 {
				return nil
			}
			return fmt.Errorf("database %s not created", dbName)
		}
		time.Sleep(time.Millisecond * 500)
	}
	return fmt.Errorf("db %s not created after waiting", dbName)
}
