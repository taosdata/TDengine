package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/errors"
)

// @author: xftan
// @date: 2022/1/27 17:24
// @description: test fetch json result
func TestFetchRowJSON(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_json_wrapper")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists test_json_wrapper")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_json_wrapper"))
	defer func() {
		err = exec(conn, "drop database if exists test_json_wrapper")
		require.NoError(t, err)
	}()
	err = exec(conn, "drop table if exists test_json_wrapper.tjsonr")
	require.NoError(t, err)
	err = exec(conn, "create stable if not exists test_json_wrapper.tjsonr(ts timestamp,v int )tags(t json)")
	require.NoError(t, err)
	err = exec(conn, `insert into test_json_wrapper.tjr_1 using test_json_wrapper.tjsonr tags('{"a":1,"b":"b"}')values (now,1)`)
	require.NoError(t, err)
	err = exec(conn, `insert into test_json_wrapper.tjr_2 using test_json_wrapper.tjsonr tags('{"a":1,"c":"c"}')values (now+1s,1)`)
	require.NoError(t, err)
	err = exec(conn, `insert into test_json_wrapper.tjr_3 using test_json_wrapper.tjsonr tags('null')values (now+2s,1)`)
	require.NoError(t, err)

	res := TaosQuery(conn, `select * from test_json_wrapper.tjsonr order by ts`)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(&errors.TaosError{
			Code:   int32(code) & 0xffff,
			ErrStr: errStr,
		})
		return
	}
	numFields := TaosFieldCount(res)
	precision := TaosResultPrecision(res)
	assert.Equal(t, 3, numFields)
	headers, err := ReadColumn(res, numFields)
	assert.NoError(t, err)
	var data [][]driver.Value
	for i := 0; i < 3; i++ {
		var d []driver.Value
		row := TaosFetchRow(res)
		lengths := FetchLengths(res, numFields)
		for j := range headers.ColTypes {
			d = append(d, FetchRow(row, j, headers.ColTypes[j], lengths[j], precision))
		}
		data = append(data, d)
	}
	TaosFreeResult(res)
	t.Log(data)
	assert.Equal(t, `{"a":1,"b":"b"}`, string(data[0][2].([]byte)))
	assert.Equal(t, `{"a":1,"c":"c"}`, string(data[1][2].([]byte)))
	assert.Nil(t, data[2][2])
}

// @author: xftan
// @date: 2022/1/27 17:24
// @description:  test TS-781 error
func TestFetchRow(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	db := "test_ts_781"
	//create stable stb1 (ts timestamp, name binary(10)) tags(n int);
	//insert into tb1 using stb1 tags(1) values(now, 'log');
	//insert into tb2 using stb1 tags(2) values(now, 'test');
	//insert into tb3 using stb1 tags(3) values(now, 'db02');
	//insert into tb4 using stb1 tags(4) values(now, 'db3');
	defer func() {
		err = exec(conn, "drop database if exists "+db)
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists "+db)
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, db))
	err = exec(conn, fmt.Sprintf("create stable if not exists %s.stb1 (ts timestamp, name binary(10)) tags(n int);", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb1 using %s.stb1 tags(1)", db, db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("insert into %s.tb1 values(now, 'log');", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb2 using %s.stb1 tags(2)", db, db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb2 values(now, 'test');", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb3 using %s.stb1 tags(3)", db, db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb3 values(now, 'db02')", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb4 using %s.stb1 tags(4)", db, db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb4 values(now, 'db3');", db))
	require.NoError(t, err)
	res := TaosQuery(conn, fmt.Sprintf("select distinct(name) from %s.stb1;", db))
	code := TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	numFields := TaosFieldCount(res)
	header, err := ReadColumn(res, numFields)
	if err != nil {
		TaosFreeResult(res)
		t.Error(err)
		return
	}
	names := map[string]struct{}{
		"log":  {},
		"test": {},
		"db02": {},
		"db3":  {},
	}
	for {
		rr := TaosFetchRow(res)
		lengths := FetchLengths(res, numFields)
		if rr == nil {
			break
		}
		d := FetchRow(rr, 0, header.ColTypes[0], lengths[0])
		delete(names, d.(string))
	}
	TaosFreeResult(res)

	assert.Empty(t, names)
}

// @author: xftan
// @date: 2022/1/27 17:24
// @description: test TS-781 nchar type error
func TestFetchRowNchar(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	db := "test_ts_781_nchar"
	//create stable stb1 (ts timestamp, name nchar(10)) tags(n int);
	//insert into tb1 using stb1 tags(1) values(now, 'log');
	//insert into tb2 using stb1 tags(2) values(now, 'test');
	//insert into tb3 using stb1 tags(3) values(now, 'db02');
	//insert into tb4 using stb1 tags(4) values(now, 'db3');
	defer func() {
		err = exec(conn, "drop database if exists "+db)
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists "+db)
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, db))
	err = exec(conn, fmt.Sprintf("create stable if not exists %s.stb1 (ts timestamp, name nchar(10)) tags(n int);", db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb1 using %s.stb1 tags(1)", db, db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb2 using %s.stb1 tags(2)", db, db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb3 using %s.stb1 tags(3)", db, db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb4 using %s.stb1 tags(4)", db, db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("insert into %s.tb1 values(now, 'log');", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb2 values(now, 'test');", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb3 values(now, 'db02')", db))
	require.NoError(t, err)
	err = exec(conn, fmt.Sprintf("insert into %s.tb4 values(now, 'db3');", db))
	require.NoError(t, err)
	res := TaosQuery(conn, fmt.Sprintf("select distinct(name) from %s.stb1;", db))
	code := TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	numFields := TaosFieldCount(res)
	header, err := ReadColumn(res, numFields)
	if err != nil {
		TaosFreeResult(res)
		t.Error(err)
		return
	}
	names := map[string]struct{}{
		"log":  {},
		"test": {},
		"db02": {},
		"db3":  {},
	}
	for {
		rr := TaosFetchRow(res)
		lengths := FetchLengths(res, numFields)
		if rr == nil {
			break
		}
		d := FetchRow(rr, 0, header.ColTypes[0], lengths[0])
		delete(names, d.(string))
	}
	TaosFreeResult(res)
	assert.Empty(t, names)
}

// @author: xftan
// @date: 2023/10/13 11:28
// @description: test fetch row all type
func TestFetchRowAllType(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	db := "test_fetch_row_all"

	err = exec(conn, "drop database if exists "+db)
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists "+db)
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists "+db)
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, db))
	err = exec(conn, fmt.Sprintf(
		"create stable if not exists %s.stb1 (ts timestamp,"+
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
			"c13 nchar(20),"+
			"c14 varbinary(20),"+
			"c15 geometry(100),"+
			"c16 decimal(20,4),"+
			"c17 decimal(10,4),"+
			"c18 blob"+
			")"+
			"tags(t json)", db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb1 using %s.stb1 tags('{\"a\":1}')", db, db))
	require.NoError(t, err)
	now := time.Now()
	err = exec(conn, fmt.Sprintf("insert into %s.tb1 values('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',123456789.123,123.456,'blob');", db, now.Format(time.RFC3339Nano)))
	require.NoError(t, err)

	res := TaosQuery(conn, fmt.Sprintf("select * from %s.stb1 where ts = '%s';", db, now.Format(time.RFC3339Nano)))
	code := TaosError(res)
	if code != int(errors.SUCCESS) {
		errStr := TaosErrorStr(res)
		err := errors.NewError(code, errStr)
		t.Error(err)
		TaosFreeResult(res)
		return
	}
	numFields := TaosFieldCount(res)
	header, err := ReadColumn(res, numFields)
	if err != nil {
		TaosFreeResult(res)
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	count := 0
	result := make([]driver.Value, numFields)
	for {
		rr := TaosFetchRow(res)
		if rr == nil {
			break
		}
		count += 1
		lengths := FetchLengths(res, numFields)

		for i := range header.ColTypes {
			result[i] = FetchRow(rr, i, header.ColTypes[i], lengths[i], precision)
		}
	}
	TaosFreeResult(res)
	assert.Equal(t, 1, count)
	assert.Equal(t, now.UnixNano()/1e6, result[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, result[1].(bool))
	assert.Equal(t, int8(2), result[2].(int8))
	assert.Equal(t, int16(3), result[3].(int16))
	assert.Equal(t, int32(4), result[4].(int32))
	assert.Equal(t, int64(5), result[5].(int64))
	assert.Equal(t, uint8(6), result[6].(uint8))
	assert.Equal(t, uint16(7), result[7].(uint16))
	assert.Equal(t, uint32(8), result[8].(uint32))
	assert.Equal(t, uint64(9), result[9].(uint64))
	assert.Equal(t, float32(10), result[10].(float32))
	assert.Equal(t, float64(11), result[11].(float64))
	assert.Equal(t, "binary", result[12].(string))
	assert.Equal(t, "nchar", result[13].(string))
	assert.Equal(t, []byte("varbinary"), result[14].([]byte))
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, result[15].([]byte))
	assert.Equal(t, "123456789.1230", result[16].(string))
	assert.Equal(t, "123.4560", result[17].(string))
	assert.Equal(t, []byte("blob"), result[18].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), result[19].([]byte))
}
