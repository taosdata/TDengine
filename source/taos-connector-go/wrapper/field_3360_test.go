package wrapper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
)

func TestReadColumn_3360(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	database := "test_read_column_3360"
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, fmt.Sprintf("drop database if exists %s", database))
		assert.NoError(t, err)
	}()
	err = exec(conn, fmt.Sprintf("create database if not exists %s", database))
	assert.NoError(t, err)
	err = exec(conn, fmt.Sprintf("use %s", database))
	assert.NoError(t, err)
	err = exec(conn, "create table if not exists alltype(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4)) tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, "create table if not exists alltype2(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(10,4)) tags (info json)")
	assert.NoError(t, err)
	err = exec(conn, `insert into t1 using alltype tags ('{"a":1}') values(now,true,2,3,4,5,6,7,8,9,10.1,11.1,'12345678901','1234567','\xaabbcc','POINT(1 1)',12.1)`)
	assert.NoError(t, err)
	res := TaosQuery(conn, "select * from alltype")
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	defer TaosFreeResult(res)
	count := TaosNumFields(res)
	assert.Equal(t, 18, count)
	ha, err := ReadColumn(res, count)
	assert.NoError(t, err)
	assert.Equal(t, 18, len(ha.ColNames))
	expect := &RowsHeader{
		ColNames: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "info"},
		ColTypes: []uint8{
			common.TSDB_DATA_TYPE_TIMESTAMP,
			common.TSDB_DATA_TYPE_BOOL,
			common.TSDB_DATA_TYPE_TINYINT,
			common.TSDB_DATA_TYPE_SMALLINT,
			common.TSDB_DATA_TYPE_INT,
			common.TSDB_DATA_TYPE_BIGINT,
			common.TSDB_DATA_TYPE_UTINYINT,
			common.TSDB_DATA_TYPE_USMALLINT,
			common.TSDB_DATA_TYPE_UINT,
			common.TSDB_DATA_TYPE_UBIGINT,
			common.TSDB_DATA_TYPE_FLOAT,
			common.TSDB_DATA_TYPE_DOUBLE,
			common.TSDB_DATA_TYPE_BINARY,
			common.TSDB_DATA_TYPE_NCHAR,
			common.TSDB_DATA_TYPE_VARBINARY,
			common.TSDB_DATA_TYPE_GEOMETRY,
			common.TSDB_DATA_TYPE_DECIMAL,
			common.TSDB_DATA_TYPE_JSON,
		},
		ColLength:  []int64{8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 20, 20, 20, 100, 16, 4095},
		Precisions: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0},
		Scales:     []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
	}
	assert.Equal(t, expect, ha)

	res2 := TaosQuery(conn, "select * from alltype2")
	code = TaosError(res2)
	if code != 0 {
		errStr := TaosErrorStr(res2)
		TaosFreeResult(res2)
		t.Error(errors.NewError(code, errStr))
		return
	}
	defer TaosFreeResult(res2)
	count = TaosNumFields(res2)
	assert.Equal(t, 18, count)
	ha, err = ReadColumn(res2, count)
	assert.NoError(t, err)
	assert.Equal(t, 18, len(ha.ColNames))
	expect = &RowsHeader{
		ColNames: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15", "v16", "info"},
		ColTypes: []uint8{
			common.TSDB_DATA_TYPE_TIMESTAMP,
			common.TSDB_DATA_TYPE_BOOL,
			common.TSDB_DATA_TYPE_TINYINT,
			common.TSDB_DATA_TYPE_SMALLINT,
			common.TSDB_DATA_TYPE_INT,
			common.TSDB_DATA_TYPE_BIGINT,
			common.TSDB_DATA_TYPE_UTINYINT,
			common.TSDB_DATA_TYPE_USMALLINT,
			common.TSDB_DATA_TYPE_UINT,
			common.TSDB_DATA_TYPE_UBIGINT,
			common.TSDB_DATA_TYPE_FLOAT,
			common.TSDB_DATA_TYPE_DOUBLE,
			common.TSDB_DATA_TYPE_BINARY,
			common.TSDB_DATA_TYPE_NCHAR,
			common.TSDB_DATA_TYPE_VARBINARY,
			common.TSDB_DATA_TYPE_GEOMETRY,
			common.TSDB_DATA_TYPE_DECIMAL64,
			common.TSDB_DATA_TYPE_JSON,
		},
		ColLength:  []int64{8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 20, 20, 20, 100, 8, 4095},
		Precisions: []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0},
		Scales:     []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0},
	}
	assert.Equal(t, expect, ha)
}
