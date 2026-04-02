package af

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common/stmt"
)

func TestStmt2_3360(t *testing.T) {
	database := "stmt2_prepare_test_3360"
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
	_, err = exec(conn, fmt.Sprintf("create database if not exists %s", database))
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_, err = exec(conn, fmt.Sprintf("drop database if exists %s", database))
		assert.NoError(t, err)
	}()
	_, err = exec(conn, fmt.Sprintf("use %s", database))
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
		"v15 nchar(20)"+
		") tags(tg binary(20))")
	assert.NoError(t, err)
	err = stmt2.Prepare("insert into ? using all_type tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if !assert.NoError(t, err) {
		return
	}
	now := time.Now().Round(time.Millisecond)
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

	err = stmt2.Prepare("select * from all_type where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ?")
	if !assert.NoError(t, err) {
		return
	}
	queryParams := []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
				{true},
				{int8(11)},
				{int16(11)},
				{int32(11)},
				{int64(11)},
				{uint8(11)},
				{uint16(11)},
				{uint32(11)},
				{uint64(11)},
				{float32(11.2)},
				{float64(11.2)},
				{"binary1"},
				{[]byte("varbinary1")},
				{"point(100 100)"},
				{"nchar1"},
			},
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
	dest := make([]driver.Value, 17)
	err = result.Next(dest)
	assert.NoError(t, err)
	for i, col := range params[0].Cols {
		assert.Equal(t, col[0], dest[i])
	}
	assert.Equal(t, "中文 tag", dest[16])
	err = result.Next(dest)
	assert.ErrorIs(t, err, io.EOF)

}
