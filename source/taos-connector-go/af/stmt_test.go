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
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func TestNewStmt(t *testing.T) {
	db := testDatabase(t)
	defer func() {
		err := db.Close()
		assert.NoError(t, err)
	}()
	_, err := exec(db, "create table test_stmt (ts timestamp,v int)")
	assert.NoError(t, err)
	stmt := db.Stmt()
	err = stmt.Prepare("insert into ? values(?,?)")
	assert.NoError(t, err)
	err = stmt.SetTableName("test_stmt")
	assert.NoError(t, err)
	ts := time.Now().UnixNano() / 1e3
	err = stmt.BindRow(param.NewParam(2).AddTimestamp(time.Unix(0, ts*1e3), common.PrecisionMicroSecond).AddInt(1))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	affected := stmt.GetAffectedRows()
	assert.Equal(t, int(1), affected)
	err = stmt.Prepare("select * from test_stmt where v = ?")
	assert.NoError(t, err)
	err = stmt.BindRow(param.NewParam(1).AddInt(1))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	rows, err := stmt.UseResult()
	assert.NoError(t, err)
	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	assert.NoError(t, err)
	assert.Equal(t, ts, dest[0].(time.Time).UnixNano()/1e3)
	assert.Equal(t, int32(1), dest[1].(int32))
	err = rows.Next(dest)
	assert.ErrorIs(t, err, io.EOF)
	err = rows.Close()
	assert.NoError(t, err)
	err = stmt.Close()
	assert.NoError(t, err)
}

func TestStmtQueryResultWithDecimal(t *testing.T) {
	conn, err := Open("", "root", "taosdata", "", 0)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()
	stmt := conn.Stmt()
	if stmt == nil {
		t.Errorf("Expected stmt to be not nil")
		return
	}
	defer func() {
		err = stmt.Close()
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "create database if not exists stmt_decimal_test")
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, "drop database if exists stmt_decimal_test")
		assert.NoError(t, err)
	}()
	_, err = exec(conn, "use stmt_decimal_test")
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
	err = stmt.Prepare("select * from ctb where ts = ?")
	if !assert.NoError(t, err) {
		return
	}
	err = stmt.BindRow(param.NewParam(1).AddTimestamp(now, common.PrecisionMilliSecond))
	if !assert.NoError(t, err) {
		return
	}
	err = stmt.Execute()
	if !assert.NoError(t, err) {
		return
	}
	result, err := stmt.UseResult()
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

func TestStmtTimezone(t *testing.T) {
	_, is3360 := os.LookupEnv("TD_3360_TEST")
	db := testDatabase(t)
	defer func() {
		err := db.Close()
		assert.NoError(t, err)
	}()
	tz := "Europe/Paris"
	timezone, err := time.LoadLocation(tz)
	require.NoError(t, err)
	err = db.SetTimezone(tz)
	require.NoError(t, err)
	_, err = exec(db, "create table test_stmt_timezone (ts timestamp,v int)")
	assert.NoError(t, err)
	stmt := db.Stmt()
	err = stmt.Prepare("insert into ? values(?,?)")
	assert.NoError(t, err)
	err = stmt.SetTableName("test_stmt_timezone")
	assert.NoError(t, err)
	var now time.Time
	if is3360 {
		now = time.Now().Round(time.Millisecond)
	} else {
		now = time.Now().In(timezone).Round(time.Millisecond)
	}
	err = stmt.BindRow(param.NewParam(2).AddTimestamp(now, common.PrecisionMicroSecond).AddInt(1))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	affected := stmt.GetAffectedRows()
	assert.Equal(t, int(1), affected)
	err = stmt.Prepare("select * from test_stmt_timezone where ts = ?")
	assert.NoError(t, err)
	t.Log(now.Format("2006-01-02 15:04:05.000"))
	err = stmt.BindRow(param.NewParam(1).AddBinary([]byte(now.Format("2006-01-02 15:04:05.000"))))
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Execute()
	assert.NoError(t, err)
	rows, err := stmt.UseResult()
	assert.NoError(t, err)
	dest := make([]driver.Value, 2)
	err = rows.Next(dest)
	assert.NoError(t, err)
	assert.Equal(t, timezone, dest[0].(time.Time).Location())
	assert.Equal(t, now.UnixNano(), dest[0].(time.Time).UnixNano())
	assert.Equal(t, int32(1), dest[1].(int32))
	err = rows.Next(dest)
	assert.ErrorIs(t, err, io.EOF)
	err = rows.Close()
	assert.NoError(t, err)
	err = stmt.Close()
	assert.NoError(t, err)
}
