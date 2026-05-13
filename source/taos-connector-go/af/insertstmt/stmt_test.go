package insertstmt

import (
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func prepareEnv(conn unsafe.Pointer) error {
	sqls := []string{
		"create database if not exists insert_stmt",
		"use insert_stmt",
		"create table test (ts timestamp, a int, b float)",
		"create table stb(ts timestamp, v int) tags(a binary(10))",
	}
	for i := 0; i < len(sqls); i++ {
		if err := exec(conn, sqls[i]); err != nil {
			return err
		}
	}
	return nil
}
func cleanEnv(conn unsafe.Pointer) error {
	sqls := []string{
		"drop database if exists insert_stmt",
	}
	for i := 0; i < len(sqls); i++ {
		if err := exec(conn, sqls[i]); err != nil {
			return err
		}
	}
	return nil
}
func TestStmt(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	err = prepareEnv(conn)
	assert.NoError(t, err)
	defer func() {
		err := cleanEnv(conn)
		if err != nil {
			t.Errorf("clean env failed: %v", err)
		}
	}()
	s := NewInsertStmt(conn)
	defer func() {
		err := s.Close()
		if err != nil {
			t.Errorf("close stmt failed: %v", err)
		}
	}()
	err = s.Prepare("insert into ? values(?,?,?)")
	assert.NoError(t, err)
	err = s.SetTableName("test")
	assert.NoError(t, err)
	params := []*param.Param{
		param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMilliSecond),
		param.NewParam(1).AddInt(1),
		param.NewParam(1).AddFloat(1.1),
	}
	err = s.BindParam(params, param.NewColumnType(3).AddTimestamp().AddInt().AddFloat())
	assert.NoError(t, err)
	err = s.AddBatch()
	assert.NoError(t, err)
	err = s.Execute()
	assert.NoError(t, err)
	affected := s.GetAffectedRows()
	assert.Equal(t, int(1), affected)

	err = s.Prepare("insert into ? using stb tags(?) values(?,?)")
	assert.NoError(t, err)
	err = s.SetTableNameWithTags("ctb1", param.NewParam(1).AddBinary([]byte("test")))
	assert.NoError(t, err)
	params = []*param.Param{
		param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMilliSecond),
		param.NewParam(1).AddInt(1),
	}
	err = s.BindParam(params, param.NewColumnType(2).AddTimestamp().AddInt())
	assert.NoError(t, err)
	err = s.AddBatch()
	assert.NoError(t, err)
	err = s.Execute()
	assert.NoError(t, err)
	affected = s.GetAffectedRows()
	assert.Equal(t, int(1), affected)

	err = s.Prepare("insert into ? using stb tags('ctb2') values(?,?)")
	assert.NoError(t, err)
	err = s.SetSubTableName("ctb2")
	assert.NoError(t, err)
	params = []*param.Param{
		param.NewParam(1).AddTimestamp(time.Now(), common.PrecisionMilliSecond),
		param.NewParam(1).AddInt(1),
	}
	err = s.BindParam(params, param.NewColumnType(2).AddTimestamp().AddInt())
	assert.NoError(t, err)
	err = s.AddBatch()
	assert.NoError(t, err)
	err = s.Execute()
	assert.NoError(t, err)
	affected = s.GetAffectedRows()
	assert.Equal(t, int(1), affected)

}
func TestStmtWithWithReqID(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	s := NewInsertStmt(conn)
	defer func() {
		err := s.Close()
		if err != nil {
			t.Errorf("close stmt failed: %v", err)
		}
	}()
	err = s.Prepare("insert into ? values(?,?,?)")
	assert.NoError(t, err)
}

func TestPrepareError(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	s := NewInsertStmt(conn)
	stmtHandle := s.stmt
	defer wrapper.TaosStmtClose(stmtHandle)
	s.stmt = nil
	err = s.Prepare("insert into ? values(?,?,?)")
	assert.Error(t, err)
	s.stmt = stmtHandle
	err = s.Prepare("select * from information_schema.ins_databases where name = ?")
	assert.Error(t, err)
}

func TestSetTableNameError(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	s := NewInsertStmt(conn)
	stmtHandle := s.stmt
	defer wrapper.TaosStmtClose(stmtHandle)
	s.stmt = nil
	err = s.SetTableName("test")
	assert.Error(t, err)

	err = s.SetSubTableName("test")
	assert.Error(t, err)

	err = s.SetTableNameWithTags("test", param.NewParam(1).AddBinary([]byte("test")))
	assert.Error(t, err)
}

func TestAddBatchError(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	s := NewInsertStmt(conn)
	stmtHandle := s.stmt
	defer wrapper.TaosStmtClose(stmtHandle)
	s.stmt = nil
	err = s.AddBatch()
	assert.Error(t, err)
}

func TestExecuteError(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	s := NewInsertStmt(conn)
	stmtHandle := s.stmt
	defer wrapper.TaosStmtClose(stmtHandle)
	s.stmt = nil
	err = s.Execute()
	assert.Error(t, err)
}

func exec(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	if code := wrapper.TaosError(res); code != 0 {
		if (code & 0xffff) == 0x3d3 {
			//Conflict transaction not completed, retry in 100ms
			wrapper.TaosFreeResult(res)
			time.Sleep(100 * time.Millisecond)
			return exec(conn, sql)
		}
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		return taosError.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(res)
	return nil
}
