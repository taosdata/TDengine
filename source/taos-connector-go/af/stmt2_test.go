package af

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common/stmt"
)

func TestStmt2CallBackCallerPool(t *testing.T) {
	pool := NewStmt2CallBackCallerPool(2)

	// Test Get method
	handle1, caller1 := pool.Get()
	if handle1 == 0 || caller1 == nil {
		t.Errorf("Expected handle and caller to be not nil")
	}

	handle2, caller2 := pool.Get()
	if handle2 == 0 || caller2 == nil {
		t.Errorf("Expected handle and caller to be not nil")
	}

	// Test Put method
	pool.Put(handle1)
	pool.Put(handle2)

	// Test Get method after Put
	handle3, caller3 := pool.Get()
	if handle3 == 0 || caller3 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle1, handle3)

	handle4, caller4 := pool.Get()
	if handle4 == 0 || caller4 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle2, handle4)

	handle5, caller5 := pool.Get()
	if handle5 == 0 || caller5 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}

	pool.Put(handle5)
	pool.Put(handle4)
	pool.Put(handle3)

	handle6, caller6 := pool.Get()
	if handle6 == 0 || caller6 == nil {
		t.Errorf("Expected handle and caller to be not nil after Put")
	}
	assert.Equal(t, handle5, handle6)
}

func TestNewStmt2(t *testing.T) {
	conn, err := Open("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	stmt := conn.Stmt2(0x12345678, false)
	if stmt == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	assert.NotNil(t, stmt.stmt2)
	err = stmt.Close()
	assert.NoError(t, err)
}

func TestStmt2(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	conn, err := Open("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	stmt2 := conn.Stmt2(0x12345678, true)
	if stmt2 == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	defer func() {
		err = stmt2.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "create database if not exists stmt2_prepare_test")
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, "drop database if exists stmt2_prepare_test")
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "use stmt2_prepare_test")
	if !assert.NoError(t, err) {
		return
	}
	_, err = exec(conn, "create table if not exists all_type("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20), "+
		"v16 blob"+
		") tags(tg binary(20))")
	assert.NoError(t, err)
	err = stmt2.Prepare("insert into ? using all_type tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if !assert.NoError(t, err) {
		return
	}
	now := time.Now().Round(time.Millisecond)
	var largeBlob = make([]byte, 1024*1024) // 1MB blob
	for i := 0; i < len(largeBlob); i++ {
		largeBlob[i] = 'a'
	}
	params := []*stmt.TaosStmt2BindData{
		{
			TableName: "中文0",
			Tags:      []driver.Value{[]byte("中文 tag")},
			Cols: [][]driver.Value{
				{
					// TIMESTAMP
					now,
					now.Add(time.Second),
					now.Add(time.Second * 2),
				},
				{
					// BOOL
					true,
					nil,
					false,
				},
				{
					// TINYINT
					int8(11),
					nil,
					int8(12),
				},
				{
					// SMALLINT
					int16(11),
					nil,
					int16(12),
				},
				{
					// INT
					int32(11),
					nil,
					int32(12),
				},
				{
					// BIGINT
					int64(11),
					nil,
					int64(12),
				},
				{
					// TINYINT UNSIGNED
					uint8(11),
					nil,
					uint8(12),
				},
				{
					// SMALLINT UNSIGNED
					uint16(11),
					nil,
					uint16(12),
				},
				{
					// INT UNSIGNED
					uint32(11),
					nil,
					uint32(12),
				},
				{
					// BIGINT UNSIGNED
					uint64(11),
					nil,
					uint64(12),
				},
				{
					// FLOAT
					float32(11.2),
					nil,
					float32(12.2),
				},
				{
					// DOUBLE
					float64(11.2),
					nil,
					float64(12.2),
				},
				{
					// BINARY
					"binary1",
					nil,
					"binary2",
				},
				{
					// VARBINARY
					[]byte("varbinary1"),
					nil,
					[]byte("varbinary2"),
				},
				{
					// GEOMETRY `point(100 100)`
					[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					nil,
					[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				},
				{
					// NCHAR
					"nchar1",
					nil,
					"nchar2",
				},
				{
					largeBlob,
					nil,
					largeBlob,
				},
			},
		},
	}
	err = stmt2.Bind(params)
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Execute()
	if !assert.NoError(t, err) {
		return
	}
	affectedRows := stmt2.GetAffectedRows()
	if !assert.Equal(t, 3, affectedRows) {
		return
	}
	// blob not support in stmt2 query
	//err = stmt2.Prepare("select * from all_type where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ? and v16 = ?")
	err = stmt2.Prepare("select * from all_type where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ?")
	if !assert.NoError(t, err) {
		return
	}
	expect := []driver.Value{
		now.Add(time.Second * 2),
		false,
		int8(12),
		int16(12),
		int32(12),
		int64(12),
		uint8(12),
		uint16(12),
		uint32(12),
		uint64(12),
		float32(12.2),
		float64(12.2),
		"binary2",
		[]byte("varbinary2"),
		[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
		"nchar2",
		largeBlob,
	}
	bind := [][]driver.Value{
		{now.Add(time.Second * 2)},
		{false},
		{int8(12)},
		{int16(12)},
		{int32(12)},
		{int64(12)},
		{uint8(12)},
		{uint16(12)},
		{uint32(12)},
		{uint64(12)},
		{float32(12.2)},
		{float64(12.2)},
		{"binary2"},
		{[]byte("varbinary2")},
		{"point(100 100)"},
		{"nchar2"},
	}
	queryParams := []*stmt.TaosStmt2BindData{
		{
			Cols: bind,
		},
	}
	err = stmt2.Bind(queryParams)
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Execute()
	if !assert.NoError(t, err) {
		return
	}
	result, err := stmt2.UseResult()
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = result.Close()
		assert.NoError(t, err)
	}()
	dest := make([]driver.Value, 18)
	err = result.Next(dest)
	assert.NoError(t, err)
	for i, col := range expect {
		assert.Equal(t, col, dest[i])
	}
	assert.Equal(t, "中文 tag", dest[17])
	err = result.Next(dest)
	assert.ErrorIs(t, err, io.EOF)

}

func TestStmt2_Prepare(t *testing.T) {
	_, is3360 := os.LookupEnv("TD_3360_TEST")
	conn, err := Open("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	stmt2 := conn.Stmt2(0x123456789, false)
	if stmt2 == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	defer func() {
		err = stmt2.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "create database if not exists stmt2_prepare_wrong_test")
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, "drop database if exists stmt2_prepare_wrong_test")
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "use stmt2_prepare_wrong_test")
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Prepare("insert into not_exist_table values(?,?,?)")
	assert.Error(t, err)
	_, err = exec(conn, "create table t (ts timestamp, b int, c int)")
	assert.NoError(t, err)
	err = stmt2.Prepare("")
	if is3360 {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
	}
	err = stmt2.Bind([]*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{time.Now()},
				{int32(1)},
				{int32(2)},
			},
		},
	})
	assert.Error(t, err)
	err = stmt2.Prepare("insert into t values(?,?,?)")
	if is3360 {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func TestStmt2QueryResultWithDecimal(t *testing.T) {
	conn, err := Open("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	stmt2 := conn.Stmt2(0x12345678, false)
	if stmt2 == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	defer func() {
		err = stmt2.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "create database if not exists stmt2_decimal_test")
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, "drop database if exists stmt2_decimal_test")
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "use stmt2_decimal_test")
	if !assert.NoError(t, err) {
		return
	}
	_, err = exec(conn, "create table if not exists ctb(ts timestamp, v1 decimal(8, 4), v2 decimal(30, 5))")
	if !assert.NoError(t, err) {
		return
	}
	now := time.Now().Round(time.Millisecond)
	ts := now.UnixNano() / 1e6
	_, err = exec(conn, fmt.Sprintf("insert into ctb values(%d,123.45,12345678901234567890.123)", ts))
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Prepare("select * from ctb where ts = ?")
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Bind([]*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Execute()
	if !assert.NoError(t, err) {
		return
	}
	result, err := stmt2.UseResult()
	if !assert.NoError(t, err) {
		return
	}
	var data = make([]driver.Value, 3)
	err = result.Next(data)
	assert.NoError(t, err)
	t.Log(data)
	assert.Equal(t, data[1].(string), "123.4500")
	assert.Equal(t, data[2].(string), "12345678901234567890.12300")
	err = result.Next(data)
	assert.ErrorIs(t, err, io.EOF)
}

func TestStmt2Timezone(t *testing.T) {
	conn, err := Open("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	tz := "Europe/Paris"
	timezone, err := time.LoadLocation(tz)
	require.NoError(t, err)
	err = conn.SetTimezone(tz)
	require.NoError(t, err)
	stmt2 := conn.Stmt2(0x12345678, false)
	if stmt2 == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	defer func() {
		err = stmt2.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "create database if not exists stmt2_timezone_test")
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, "drop database if exists stmt2_timezone_test")
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "use stmt2_timezone_test")
	if !assert.NoError(t, err) {
		return
	}
	_, err = exec(conn, "create table if not exists ctb(ts timestamp, v1 int)")
	if !assert.NoError(t, err) {
		return
	}
	now := time.Now().Round(time.Millisecond)
	ts := now.UnixNano() / 1e6
	_, err = exec(conn, fmt.Sprintf("insert into ctb values(%d,1)", ts))
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Prepare("select * from ctb where ts = ?")
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Bind([]*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	err = stmt2.Execute()
	if !assert.NoError(t, err) {
		return
	}
	result, err := stmt2.UseResult()
	if !assert.NoError(t, err) {
		return
	}
	var data = make([]driver.Value, 2)
	err = result.Next(data)
	assert.NoError(t, err)
	t.Log(data)
	assert.Equal(t, data[0].(time.Time).Location(), timezone)
	assert.Equal(t, now.UnixNano(), data[0].(time.Time).UnixNano())
	assert.Equal(t, int32(1), data[1].(int32))
	err = result.Next(data)
	assert.ErrorIs(t, err, io.EOF)
}
