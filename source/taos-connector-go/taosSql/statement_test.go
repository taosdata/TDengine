package taosSql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test stmt exec
func TestStmtExec(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}(db)
	defer func() {
		_, err = exec(db, "drop database if exists test_stmt_driver")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = exec(db, "create database if not exists test_stmt_driver")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, "create table if not exists test_stmt_driver.ct(ts timestamp,"+
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
	stmt, err := db.Prepare("insert into test_stmt_driver.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

	if err != nil {
		t.Error(err)
		return
	}
	result, err := stmt.Exec(time.Now(), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		t.Error(err)
		return
	}
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

func TestStmtQuery(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}(db)
	defer func() {
		_, err := exec(db, "drop database if exists test_stmt_driver_q")
		assert.NoError(t, err)
	}()
	_, err = exec(db, "create database if not exists test_stmt_driver_q")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, "create table if not exists test_stmt_driver_q.ct(ts timestamp,"+
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
	stmt, err := db.Prepare("insert into test_stmt_driver_q.ct values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now()
	result, err := stmt.Exec(now, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, "binary", "nchar")
	if err != nil {
		t.Error(err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, int64(1), affected)
	err = stmt.Close()
	assert.NoError(t, err)
	stmt, err = db.Prepare("select * from test_stmt_driver_q.ct where ts = ?")
	if err != nil {
		t.Error(err)
		return
	}
	rows, err := stmt.Query(now)
	if err != nil {
		t.Error(err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, []string{"ts", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"}, columns)
	count := 0
	for rows.Next() {
		count += 1
		var (
			ts  time.Time
			c1  bool
			c2  int8
			c3  int16
			c4  int32
			c5  int64
			c6  uint8
			c7  uint16
			c8  uint32
			c9  uint64
			c10 float32
			c11 float64
			c12 string
			c13 string
		)
		err = rows.Scan(&ts,
			&c1,
			&c2,
			&c3,
			&c4,
			&c5,
			&c6,
			&c7,
			&c8,
			&c9,
			&c10,
			&c11,
			&c12,
			&c13)
		assert.NoError(t, err)
		assert.Equal(t, now.UnixNano()/1e6, ts.UnixNano()/1e6)
		assert.Equal(t, true, c1)
		assert.Equal(t, int8(2), c2)
		assert.Equal(t, int16(3), c3)
		assert.Equal(t, int32(4), c4)
		assert.Equal(t, int64(5), c5)
		assert.Equal(t, uint8(6), c6)
		assert.Equal(t, uint16(7), c7)
		assert.Equal(t, uint32(8), c8)
		assert.Equal(t, uint64(9), c9)
		assert.Equal(t, float32(10), c10)
		assert.Equal(t, float64(11), c11)
		assert.Equal(t, "binary", c12)
		assert.Equal(t, "nchar", c13)
	}
	assert.Equal(t, 1, count)
}

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test stmt convert
func TestStmtConvertExec(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(db, "drop database if exists test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_, err = exec(db, "drop database if exists test_stmt_driver_convert")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = exec(db, "create database test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, "use test_stmt_driver_convert")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Format(time.RFC3339Nano)
	tests := []struct {
		name        string
		tbType      string
		pos         string
		bind        []interface{}
		expectValue interface{}
		expectError bool
	}{
		//bool
		{
			name:        "bool_null",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "bool_err",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, []int{123}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "bool_bool_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: true,
		},
		{
			name:        "bool_bool_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: false,
		},
		{
			name:        "bool_float_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: true,
		},
		{
			name:        "bool_float_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, float32(0)},
			expectValue: false,
		},
		{
			name:        "bool_int_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, int32(1)},
			expectValue: true,
		},
		{
			name:        "bool_int_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, int32(0)},
			expectValue: false,
		},
		{
			name:        "bool_uint_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, uint32(1)},
			expectValue: true,
		},
		{
			name:        "bool_uint_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, uint32(0)},
			expectValue: false,
		},
		{
			name:        "bool_string_true",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, "true"},
			expectValue: true,
		},
		{
			name:        "bool_string_false",
			tbType:      "ts timestamp,v bool",
			pos:         "?,?",
			bind:        []interface{}{now, "false"},
			expectValue: false,
		},
		//tiny int
		{
			name:        "tiny_nil",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "tiny_err",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "tiny_bool_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int8(1),
		},
		{
			name:        "tiny_bool_0",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int8(0),
		},
		{
			name:        "tiny_float_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_int_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_uint_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int8(1),
		},
		{
			name:        "tiny_string_1",
			tbType:      "ts timestamp,v tinyint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int8(1),
		},
		// small int
		{
			name:        "small_nil",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "small_err",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "small_bool_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int16(1),
		},
		{
			name:        "small_bool_0",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int16(0),
		},
		{
			name:        "small_float_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_int_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_uint_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int16(1),
		},
		{
			name:        "small_string_1",
			tbType:      "ts timestamp,v smallint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int16(1),
		},
		// int
		{
			name:        "int_nil",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "int_err",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "int_bool_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int32(1),
		},
		{
			name:        "int_bool_0",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int32(0),
		},
		{
			name:        "int_float_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_int_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_uint_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int32(1),
		},
		{
			name:        "int_string_1",
			tbType:      "ts timestamp,v int",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int32(1),
		},
		// big int
		{
			name:        "big_nil",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "big_err",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "big_bool_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: int64(1),
		},
		{
			name:        "big_bool_0",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: int64(0),
		},
		{
			name:        "big_float_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_int_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_uint_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: int64(1),
		},
		{
			name:        "big_string_1",
			tbType:      "ts timestamp,v bigint",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: int64(1),
		},
		// float
		{
			name:        "float_nil",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "float_err",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "float_bool_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: float32(1),
		},
		{
			name:        "float_bool_0",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: float32(0),
		},
		{
			name:        "float_float_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_int_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_uint_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: float32(1),
		},
		{
			name:        "float_string_1",
			tbType:      "ts timestamp,v float",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: float32(1),
		},
		//double
		{
			name:        "double_nil",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "double_err",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "double_bool_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: float64(1),
		},
		{
			name:        "double_bool_0",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: float64(0),
		},
		{
			name:        "double_double_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_int_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_uint_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: float64(1),
		},
		{
			name:        "double_string_1",
			tbType:      "ts timestamp,v double",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: float64(1),
		},

		//tiny int unsigned
		{
			name:        "utiny_nil",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "utiny_err",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "utiny_bool_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_bool_0",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint8(0),
		},
		{
			name:        "utiny_float_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_int_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_uint_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint8(1),
		},
		{
			name:        "utiny_string_1",
			tbType:      "ts timestamp,v tinyint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint8(1),
		},
		// small int unsigned
		{
			name:        "usmall_nil",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "usmall_err",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "usmall_bool_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_bool_0",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint16(0),
		},
		{
			name:        "usmall_float_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_int_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_uint_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint16(1),
		},
		{
			name:        "usmall_string_1",
			tbType:      "ts timestamp,v smallint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint16(1),
		},
		// int unsigned
		{
			name:        "uint_nil",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "uint_err",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "uint_bool_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint32(1),
		},
		{
			name:        "uint_bool_0",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint32(0),
		},
		{
			name:        "uint_float_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_int_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_uint_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint32(1),
		},
		{
			name:        "uint_string_1",
			tbType:      "ts timestamp,v int unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint32(1),
		},
		// big int unsigned
		{
			name:        "ubig_nil",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "ubig_err",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "ubig_bool_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, true},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_bool_0",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, false},
			expectValue: uint64(0),
		},
		{
			name:        "ubig_float_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_int_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_uint_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: uint64(1),
		},
		{
			name:        "ubig_string_1",
			tbType:      "ts timestamp,v bigint unsigned",
			pos:         "?,?",
			bind:        []interface{}{now, "1"},
			expectValue: uint64(1),
		},
		//binary
		{
			name:        "binary_nil",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "binary_err",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "binary_string_chinese",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, "中文"},
			expectValue: "中文",
		},
		{
			name:        "binary_bytes_chinese",
			tbType:      "ts timestamp,v binary(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []byte("中文")},
			expectValue: "中文",
		},
		//nchar
		{
			name:        "nchar_nil",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "nchar_err",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "binary_string_chinese",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, "中文"},
			expectValue: "中文",
		},
		{
			name:        "binary_bytes_chinese",
			tbType:      "ts timestamp,v nchar(24)",
			pos:         "?,?",
			bind:        []interface{}{now, []byte("中文")},
			expectValue: "中文",
		},
		// timestamp
		{
			name:        "ts_nil",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, nil},
			expectValue: nil,
		},
		{
			name:        "ts_err",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, []int{1}},
			expectValue: nil,
			expectError: true,
		},
		{
			name:        "ts_time_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, time.Unix(0, 1e6)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_float_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, float32(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_int_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, int(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_uint_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, uint(1)},
			expectValue: time.Unix(0, 1e6),
		},
		{
			name:        "ts_string_1",
			tbType:      "ts timestamp,v timestamp",
			pos:         "?,?",
			bind:        []interface{}{now, "1970-01-01T00:00:00.001Z"},
			expectValue: time.Unix(0, 1e6),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbName := fmt.Sprintf("test_%s", tt.name)
			tbType := tt.tbType
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tt.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if _, err = exec(db, drop); err != nil {
				t.Error(err)
				return
			}
			if _, err = exec(db, create); err != nil {
				t.Error(err)
				return
			}
			stmt, err := db.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			result, err := stmt.Exec(tt.bind...)
			if tt.expectError {
				assert.NotNil(t, err)
				err = stmt.Close()
				assert.NoError(t, err)
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			affected, err := result.RowsAffected()
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, int64(1), affected)
			rows, err := db.Query(fmt.Sprintf("select v from %s", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			var data []driver.Value
			tts, err := rows.ColumnTypes()
			if err != nil {
				t.Error(err)
				return
			}
			typesL := make([]reflect.Type, 1)
			for i, tp := range tts {
				st := tp.ScanType()
				if st == nil {
					t.Errorf("scantype is null for column %q", tp.Name())
					continue
				}
				typesL[i] = st
			}
			for rows.Next() {
				values := make([]interface{}, 1)
				for i := range values {
					values[i] = reflect.New(typesL[i]).Interface()
				}
				err = rows.Scan(values...)
				if err != nil {
					t.Error(err)
					return
				}
				v, err := values[0].(driver.Valuer).Value()
				if err != nil {
					t.Error(err)
				}
				data = append(data, v)
			}
			if len(data) != 1 {
				t.Errorf("expect %d got %d", 1, len(data))
				return
			}
			if data[0] != tt.expectValue {
				t.Errorf("expect %v got %v", tt.expectValue, data[0])
				return
			}
		})
	}
}

func TestStmtConvertQuery(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(db, "drop database if exists test_stmt_driver_convert_q")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_, err = exec(db, "drop database if exists test_stmt_driver_convert_q")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = exec(db, "create database test_stmt_driver_convert_q")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, "use test_stmt_driver_convert_q")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, "create table t0 (ts timestamp,"+
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
	now := time.Now()
	after1s := now.Add(time.Second)
	_, err = exec(db, fmt.Sprintf("insert into t0 values('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now.Format(time.RFC3339Nano)))
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, fmt.Sprintf("insert into t0 values('%s',null,null,null,null,null,null,null,null,null,null,null,null,null)", after1s.Format(time.RFC3339Nano)))
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		name          string
		field         string
		where         string
		bind          interface{}
		expectNoValue bool
		expectValue   driver.Value
		expectError   bool
	}{
		//ts
		{
			name:        "ts",
			field:       "ts",
			where:       "ts = ?",
			bind:        now,
			expectValue: time.Unix(now.Unix(), int64((now.Nanosecond()/1e6)*1e6)).Local(),
		},

		//bool
		{
			name:        "bool_true",
			field:       "c1",
			where:       "c1 = ?",
			bind:        true,
			expectValue: true,
		},
		{
			name:          "bool_false",
			field:         "c1",
			where:         "c1 = ?",
			bind:          false,
			expectNoValue: true,
		},
		{
			name:        "tinyint_int8",
			field:       "c2",
			where:       "c2 = ?",
			bind:        int8(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_iny16",
			field:       "c2",
			where:       "c2 = ?",
			bind:        int16(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_int32",
			field:       "c2",
			where:       "c2 = ?",
			bind:        int32(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_int64",
			field:       "c2",
			where:       "c2 = ?",
			bind:        int64(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_uint8",
			field:       "c2",
			where:       "c2 = ?",
			bind:        uint8(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_uint16",
			field:       "c2",
			where:       "c2 = ?",
			bind:        uint16(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_uint32",
			field:       "c2",
			where:       "c2 = ?",
			bind:        uint32(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_uint64",
			field:       "c2",
			where:       "c2 = ?",
			bind:        uint64(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_float32",
			field:       "c2",
			where:       "c2 = ?",
			bind:        float32(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_float64",
			field:       "c2",
			where:       "c2 = ?",
			bind:        float64(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_int",
			field:       "c2",
			where:       "c2 = ?",
			bind:        int(2),
			expectValue: int8(2),
		},
		{
			name:        "tinyint_uint",
			field:       "c2",
			where:       "c2 = ?",
			bind:        uint(2),
			expectValue: int8(2),
		},

		// smallint
		{
			name:        "smallint_int8",
			field:       "c3",
			where:       "c3 = ?",
			bind:        int8(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_iny16",
			field:       "c3",
			where:       "c3 = ?",
			bind:        int16(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_int32",
			field:       "c3",
			where:       "c3 = ?",
			bind:        int32(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_int64",
			field:       "c3",
			where:       "c3 = ?",
			bind:        int64(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_uint8",
			field:       "c3",
			where:       "c3 = ?",
			bind:        uint8(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_uint16",
			field:       "c3",
			where:       "c3 = ?",
			bind:        uint16(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_uint32",
			field:       "c3",
			where:       "c3 = ?",
			bind:        uint32(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_uint64",
			field:       "c3",
			where:       "c3 = ?",
			bind:        uint64(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_float32",
			field:       "c3",
			where:       "c3 = ?",
			bind:        float32(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_float64",
			field:       "c3",
			where:       "c3 = ?",
			bind:        float64(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_int",
			field:       "c3",
			where:       "c3 = ?",
			bind:        int(3),
			expectValue: int16(3),
		},
		{
			name:        "smallint_uint",
			field:       "c3",
			where:       "c3 = ?",
			bind:        uint(3),
			expectValue: int16(3),
		},

		//int
		{
			name:        "int_int8",
			field:       "c4",
			where:       "c4 = ?",
			bind:        int8(4),
			expectValue: int32(4),
		},
		{
			name:        "int_iny16",
			field:       "c4",
			where:       "c4 = ?",
			bind:        int16(4),
			expectValue: int32(4),
		},
		{
			name:        "int_int32",
			field:       "c4",
			where:       "c4 = ?",
			bind:        int32(4),
			expectValue: int32(4),
		},
		{
			name:        "int_int64",
			field:       "c4",
			where:       "c4 = ?",
			bind:        int64(4),
			expectValue: int32(4),
		},
		{
			name:        "int_uint8",
			field:       "c4",
			where:       "c4 = ?",
			bind:        uint8(4),
			expectValue: int32(4),
		},
		{
			name:        "int_uint16",
			field:       "c4",
			where:       "c4 = ?",
			bind:        uint16(4),
			expectValue: int32(4),
		},
		{
			name:        "int_uint32",
			field:       "c4",
			where:       "c4 = ?",
			bind:        uint32(4),
			expectValue: int32(4),
		},
		{
			name:        "int_uint64",
			field:       "c4",
			where:       "c4 = ?",
			bind:        uint64(4),
			expectValue: int32(4),
		},
		{
			name:        "int_float32",
			field:       "c4",
			where:       "c4 = ?",
			bind:        float32(4),
			expectValue: int32(4),
		},
		{
			name:        "int_float64",
			field:       "c4",
			where:       "c4 = ?",
			bind:        float64(4),
			expectValue: int32(4),
		},
		{
			name:        "int_int",
			field:       "c4",
			where:       "c4 = ?",
			bind:        int(4),
			expectValue: int32(4),
		},
		{
			name:        "int_uint",
			field:       "c4",
			where:       "c4 = ?",
			bind:        uint(4),
			expectValue: int32(4),
		},

		//bigint
		{
			name:        "bigint_int8",
			field:       "c5",
			where:       "c5 = ?",
			bind:        int8(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_iny16",
			field:       "c5",
			where:       "c5 = ?",
			bind:        int16(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_int32",
			field:       "c5",
			where:       "c5 = ?",
			bind:        int32(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_int64",
			field:       "c5",
			where:       "c5 = ?",
			bind:        int64(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_uint8",
			field:       "c5",
			where:       "c5 = ?",
			bind:        uint8(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_uint16",
			field:       "c5",
			where:       "c5 = ?",
			bind:        uint16(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_uint32",
			field:       "c5",
			where:       "c5 = ?",
			bind:        uint32(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_uint64",
			field:       "c5",
			where:       "c5 = ?",
			bind:        uint64(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_float32",
			field:       "c5",
			where:       "c5 = ?",
			bind:        float32(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_float64",
			field:       "c5",
			where:       "c5 = ?",
			bind:        float64(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_int",
			field:       "c5",
			where:       "c5 = ?",
			bind:        int(5),
			expectValue: int64(5),
		},
		{
			name:        "bigint_uint",
			field:       "c5",
			where:       "c5 = ?",
			bind:        uint(5),
			expectValue: int64(5),
		},

		//utinyint
		{
			name:        "utinyint_int8",
			field:       "c6",
			where:       "c6 = ?",
			bind:        int8(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_iny16",
			field:       "c6",
			where:       "c6 = ?",
			bind:        int16(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_int32",
			field:       "c6",
			where:       "c6 = ?",
			bind:        int32(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_int64",
			field:       "c6",
			where:       "c6 = ?",
			bind:        int64(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_uint8",
			field:       "c6",
			where:       "c6 = ?",
			bind:        uint8(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_uint16",
			field:       "c6",
			where:       "c6 = ?",
			bind:        uint16(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_uint32",
			field:       "c6",
			where:       "c6 = ?",
			bind:        uint32(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_uint64",
			field:       "c6",
			where:       "c6 = ?",
			bind:        uint64(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_float32",
			field:       "c6",
			where:       "c6 = ?",
			bind:        float32(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_float64",
			field:       "c6",
			where:       "c6 = ?",
			bind:        float64(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_int",
			field:       "c6",
			where:       "c6 = ?",
			bind:        int(6),
			expectValue: uint8(6),
		},
		{
			name:        "utinyint_uint",
			field:       "c6",
			where:       "c6 = ?",
			bind:        uint(6),
			expectValue: uint8(6),
		},

		//usmallint
		{
			name:        "usmallint_int8",
			field:       "c7",
			where:       "c7 = ?",
			bind:        int8(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_iny16",
			field:       "c7",
			where:       "c7 = ?",
			bind:        int16(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_int32",
			field:       "c7",
			where:       "c7 = ?",
			bind:        int32(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_int64",
			field:       "c7",
			where:       "c7 = ?",
			bind:        int64(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_uint8",
			field:       "c7",
			where:       "c7 = ?",
			bind:        uint8(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_uint16",
			field:       "c7",
			where:       "c7 = ?",
			bind:        uint16(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_uint32",
			field:       "c7",
			where:       "c7 = ?",
			bind:        uint32(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_uint64",
			field:       "c7",
			where:       "c7 = ?",
			bind:        uint64(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_float32",
			field:       "c7",
			where:       "c7 = ?",
			bind:        float32(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_float64",
			field:       "c7",
			where:       "c7 = ?",
			bind:        float64(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_int",
			field:       "c7",
			where:       "c7 = ?",
			bind:        int(7),
			expectValue: uint16(7),
		},
		{
			name:        "usmallint_uint",
			field:       "c7",
			where:       "c7 = ?",
			bind:        uint(7),
			expectValue: uint16(7),
		},

		//uint
		{
			name:        "uint_int8",
			field:       "c8",
			where:       "c8 = ?",
			bind:        int8(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_iny16",
			field:       "c8",
			where:       "c8 = ?",
			bind:        int16(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_int32",
			field:       "c8",
			where:       "c8 = ?",
			bind:        int32(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_int64",
			field:       "c8",
			where:       "c8 = ?",
			bind:        int64(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_uint8",
			field:       "c8",
			where:       "c8 = ?",
			bind:        uint8(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_uint16",
			field:       "c8",
			where:       "c8 = ?",
			bind:        uint16(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_uint32",
			field:       "c8",
			where:       "c8 = ?",
			bind:        uint32(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_uint64",
			field:       "c8",
			where:       "c8 = ?",
			bind:        uint64(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_float32",
			field:       "c8",
			where:       "c8 = ?",
			bind:        float32(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_float64",
			field:       "c8",
			where:       "c8 = ?",
			bind:        float64(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_int",
			field:       "c8",
			where:       "c8 = ?",
			bind:        int(8),
			expectValue: uint32(8),
		},
		{
			name:        "uint_uint",
			field:       "c8",
			where:       "c8 = ?",
			bind:        uint(8),
			expectValue: uint32(8),
		},

		//ubigint
		{
			name:        "ubigint_int8",
			field:       "c9",
			where:       "c9 = ?",
			bind:        int8(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_iny16",
			field:       "c9",
			where:       "c9 = ?",
			bind:        int16(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_int32",
			field:       "c9",
			where:       "c9 = ?",
			bind:        int32(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_int64",
			field:       "c9",
			where:       "c9 = ?",
			bind:        int64(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_uint8",
			field:       "c9",
			where:       "c9 = ?",
			bind:        uint8(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_uint16",
			field:       "c9",
			where:       "c9 = ?",
			bind:        uint16(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_uint32",
			field:       "c9",
			where:       "c9 = ?",
			bind:        uint32(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_uint64",
			field:       "c9",
			where:       "c9 = ?",
			bind:        uint64(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_float32",
			field:       "c9",
			where:       "c9 = ?",
			bind:        float32(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_float64",
			field:       "c9",
			where:       "c9 = ?",
			bind:        float64(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_int",
			field:       "c9",
			where:       "c9 = ?",
			bind:        int(9),
			expectValue: uint64(9),
		},
		{
			name:        "ubigint_uint",
			field:       "c9",
			where:       "c9 = ?",
			bind:        uint(9),
			expectValue: uint64(9),
		},

		//float
		{
			name:        "float_int8",
			field:       "c10",
			where:       "c10 = ?",
			bind:        int8(10),
			expectValue: float32(10),
		},
		{
			name:        "float_iny16",
			field:       "c10",
			where:       "c10 = ?",
			bind:        int16(10),
			expectValue: float32(10),
		},
		{
			name:        "float_int32",
			field:       "c10",
			where:       "c10 = ?",
			bind:        int32(10),
			expectValue: float32(10),
		},
		{
			name:        "float_int64",
			field:       "c10",
			where:       "c10 = ?",
			bind:        int64(10),
			expectValue: float32(10),
		},
		{
			name:        "float_uint8",
			field:       "c10",
			where:       "c10 = ?",
			bind:        uint8(10),
			expectValue: float32(10),
		},
		{
			name:        "float_uint16",
			field:       "c10",
			where:       "c10 = ?",
			bind:        uint16(10),
			expectValue: float32(10),
		},
		{
			name:        "float_uint32",
			field:       "c10",
			where:       "c10 = ?",
			bind:        uint32(10),
			expectValue: float32(10),
		},
		{
			name:        "float_uint64",
			field:       "c10",
			where:       "c10 = ?",
			bind:        uint64(10),
			expectValue: float32(10),
		},
		{
			name:        "float_float32",
			field:       "c10",
			where:       "c10 = ?",
			bind:        float32(10),
			expectValue: float32(10),
		},
		{
			name:        "float_float64",
			field:       "c10",
			where:       "c10 = ?",
			bind:        float64(10),
			expectValue: float32(10),
		},
		{
			name:        "float_int",
			field:       "c10",
			where:       "c10 = ?",
			bind:        int(10),
			expectValue: float32(10),
		},
		{
			name:        "float_uint",
			field:       "c10",
			where:       "c10 = ?",
			bind:        uint(10),
			expectValue: float32(10),
		},

		//double
		{
			name:        "double_int8",
			field:       "c11",
			where:       "c11 = ?",
			bind:        int8(11),
			expectValue: float64(11),
		},
		{
			name:        "double_iny16",
			field:       "c11",
			where:       "c11 = ?",
			bind:        int16(11),
			expectValue: float64(11),
		},
		{
			name:        "double_int32",
			field:       "c11",
			where:       "c11 = ?",
			bind:        int32(11),
			expectValue: float64(11),
		},
		{
			name:        "double_int64",
			field:       "c11",
			where:       "c11 = ?",
			bind:        int64(11),
			expectValue: float64(11),
		},
		{
			name:        "double_uint8",
			field:       "c11",
			where:       "c11 = ?",
			bind:        uint8(11),
			expectValue: float64(11),
		},
		{
			name:        "double_uint16",
			field:       "c11",
			where:       "c11 = ?",
			bind:        uint16(11),
			expectValue: float64(11),
		},
		{
			name:        "double_uint32",
			field:       "c11",
			where:       "c11 = ?",
			bind:        uint32(11),
			expectValue: float64(11),
		},
		{
			name:        "double_uint64",
			field:       "c11",
			where:       "c11 = ?",
			bind:        uint64(11),
			expectValue: float64(11),
		},
		{
			name:        "double_float32",
			field:       "c11",
			where:       "c11 = ?",
			bind:        float32(11),
			expectValue: float64(11),
		},
		{
			name:        "double_float64",
			field:       "c11",
			where:       "c11 = ?",
			bind:        float64(11),
			expectValue: float64(11),
		},
		{
			name:        "double_int",
			field:       "c11",
			where:       "c11 = ?",
			bind:        int(11),
			expectValue: float64(11),
		},
		{
			name:        "double_uint",
			field:       "c11",
			where:       "c11 = ?",
			bind:        uint(11),
			expectValue: float64(11),
		},

		// binary
		{
			name:        "binary_string",
			field:       "c12",
			where:       "c12 = ?",
			bind:        "binary",
			expectValue: "binary",
		},
		{
			name:        "binary_bytes",
			field:       "c12",
			where:       "c12 = ?",
			bind:        []byte("binary"),
			expectValue: "binary",
		},
		{
			name:        "binary_string_like",
			field:       "c12",
			where:       "c12 like ?",
			bind:        "bin%",
			expectValue: "binary",
		},

		// nchar
		{
			name:        "nchar_string",
			field:       "c13",
			where:       "c13 = ?",
			bind:        "nchar",
			expectValue: "nchar",
		},
		{
			name:        "nchar_bytes",
			field:       "c13",
			where:       "c13 = ?",
			bind:        []byte("nchar"),
			expectValue: "nchar",
		},
		{
			name:        "nchar_string",
			field:       "c13",
			where:       "c13 like ?",
			bind:        "nch%",
			expectValue: "nchar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := fmt.Sprintf("select %s from t0 where %s", tt.field, tt.where)

			stmt, err := db.Prepare(sql)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				err := stmt.Close()
				assert.NoError(t, err)
			}()
			rows, err := stmt.Query(tt.bind)
			if tt.expectError {
				assert.NotNil(t, err)
				err = stmt.Close()
				assert.NoError(t, err)
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			tts, err := rows.ColumnTypes()
			if err != nil {
				t.Error(err)
				return
			}
			typesL := make([]reflect.Type, 1)
			for i, tp := range tts {
				st := tp.ScanType()
				if st == nil {
					t.Errorf("scantype is null for column %q", tp.Name())
					continue
				}
				typesL[i] = st
			}
			var data []driver.Value
			for rows.Next() {
				values := make([]interface{}, 1)
				for i := range values {
					values[i] = reflect.New(typesL[i]).Interface()
				}
				err = rows.Scan(values...)
				if err != nil {
					t.Error(err)
					return
				}
				v, err := values[0].(driver.Valuer).Value()
				if err != nil {
					t.Error(err)
				}
				data = append(data, v)
			}
			if tt.expectNoValue {
				if len(data) > 0 {
					t.Errorf("expect no value got %#v", data)
					return
				}
				return
			}
			if len(data) != 1 {
				t.Errorf("expect %d got %d", 1, len(data))
				return
			}
			if data[0] != tt.expectValue {
				t.Errorf("expect %v got %v", tt.expectValue, data[0])
				return
			}
		})
	}
}

func TestWrongStmt(t *testing.T) {
	d := &TDengineDriver{}
	conn, err := d.Open(dataSourceName)
	assert.NoError(t, err)
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	c := conn.(*taosConn)
	cPointer := c.taos
	c.taos = nil
	defer func() {
		c.taos = cPointer
	}()
	_, err = c.Prepare("")
	assert.Error(t, err)

	p, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(p)
	stmt := wrapper.TaosStmtInit(p)
	code := wrapper.TaosStmtPrepare(stmt, "")
	err = checkStmtError(code, stmt)
	assert.NoError(t, err)
	code = wrapper.TaosStmtExecute(stmt)
	assert.NotEqual(t, 0, code)
	err = checkStmtError(code, stmt)
	assert.Error(t, err)
}

func TestStmtDecimal(t *testing.T) {
	testDbName := "test_stmt_decimal"
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		t.Error(err)
		return
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}(db)
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", testDbName))
		if err != nil {
			t.Error(err)
			return
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", testDbName))
	if err != nil {
		t.Error(err)
		return
	}
	_, err = exec(db, fmt.Sprintf("create table if not exists %s.ctb(ts timestamp,v decimal(8,4),v2 decimal (30,5))", testDbName))
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	ts := now.UnixNano() / 1e6
	_, err = exec(db, fmt.Sprintf("insert into %s.ctb values(%d,123.456,12345678901234567890.12345)", testDbName, ts))
	assert.NoError(t, err)
	stmt, err := db.Prepare(fmt.Sprintf("select * from %s.ctb where ts = ?", testDbName))
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = stmt.Close()
		assert.NoError(t, err)
	}()
	rows, err := stmt.Query(now)
	count := 0
	assert.NoError(t, err)
	for rows.Next() {
		var c1 time.Time
		var c2 string
		var c3 string
		err = rows.Scan(&c1, &c2, &c3)
		assert.NoError(t, err)
		count += 1
		t.Log(c1, c2, c3)
		assert.Equal(t, c2, "123.4560")
		assert.Equal(t, c3, "12345678901234567890.12345")
	}
	assert.Equal(t, 1, count)
}
