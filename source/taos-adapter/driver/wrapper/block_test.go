package wrapper

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/errors"
)

// @author: xftan
// @date: 2023/10/13 11:27
// @description: test read block
func TestReadBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_block_raw")
	assert.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_block_raw")
		assert.NoError(t, err)
	}()
	err = exec(conn, "create database test_block_raw")
	assert.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_block_raw"))
	err = exec(conn, "create table if not exists test_block_raw.all_type (ts timestamp,"+
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
		"c16 decimal(8,4),"+
		"c17 decimal(20,4),"+
		"c18 blob"+
		") tags (info json)")
	assert.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	after2s := now.Add(2 * time.Second)
	sql := fmt.Sprintf("insert into test_block_raw.t0 using test_block_raw.all_type tags('{\"a\":1}') values"+
		"('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','123456.456','blob')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)"+
		"('%s',true,%d,%d,%d,%d,%d,%d,%d,%v,%f,%f,'b','n','test_varbinary','point(100 100)','123.456','123456.456','blob')",
		now.Format(time.RFC3339Nano),
		after1s.Format(time.RFC3339Nano),
		after2s.Format(time.RFC3339Nano),
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		uint64(math.MaxUint64),
		math.MaxFloat32,
		math.MaxFloat64,
	)
	err = exec(conn, sql)
	assert.NoError(t, err)
	sql = "select * from test_block_raw.all_type"
	data, err := query(conn, sql)
	require.NoError(t, err)
	assert.Equal(t, 3, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte("test_varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, row1[15].([]byte))
	assert.Equal(t, "123.4560", row1[16].(string))
	assert.Equal(t, "123456.4560", row1[17].(string))
	assert.Equal(t, []byte("blob"), row1[18].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[19].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 19; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[19].([]byte))
	row3 := data[2]
	assert.Equal(t, after2s.UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row3[1].(bool))
	assert.Equal(t, int8(math.MaxInt8), row3[2].(int8))
	assert.Equal(t, int16(math.MaxInt16), row3[3].(int16))
	assert.Equal(t, int32(math.MaxInt32), row3[4].(int32))
	assert.Equal(t, int64(math.MaxInt64), row3[5].(int64))
	assert.Equal(t, uint8(math.MaxUint8), row3[6].(uint8))
	assert.Equal(t, uint16(math.MaxUint16), row3[7].(uint16))
	assert.Equal(t, uint32(math.MaxUint32), row3[8].(uint32))
	assert.Equal(t, uint64(math.MaxUint64), row3[9].(uint64))
	assert.Equal(t, float32(math.MaxFloat32), row3[10].(float32))
	assert.Equal(t, float64(math.MaxFloat64), row3[11].(float64))
	assert.Equal(t, "b", row3[12].(string))
	assert.Equal(t, "n", row3[13].(string))
	assert.Equal(t, []byte("test_varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, row1[15].([]byte))
	assert.Equal(t, "123.4560", row1[16].(string))
	assert.Equal(t, "123456.4560", row1[17].(string))
	assert.Equal(t, []byte("blob"), row1[18].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[19].([]byte))
}

// @author: xftan
// @date: 2023/10/13 11:27
// @description: test write raw block
func TestTaosWriteRawBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_write_block_raw")
	defer func() {
		err = exec(conn, "drop database if exists test_write_block_raw")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database test_write_block_raw")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_write_block_raw"))
	// todo: TDengine not support taos_write_raw_block with blob data
	err = exec(conn, "create table if not exists test_write_block_raw.all_type (ts timestamp,"+
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
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_write_block_raw.t0 using test_write_block_raw.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','123456.456')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "create table test_write_block_raw.t1 using test_write_block_raw.all_type tags('{\"a\":2}')"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "use test_write_block_raw"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select * from test_write_block_raw.t0"
	res := TaosQuery(conn, sql)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}

		errCode = TaosWriteRawBlock(conn, blockSize, block, "t1")
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(nil)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw.t1"
	data, err := query(conn, sql)
	require.NoError(t, err)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte("test_varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, row1[15].([]byte))
	assert.Equal(t, "123.4560", row1[16].(string))
	assert.Equal(t, "123456.4560", row1[17].(string))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 18; i++ {
		assert.Nil(t, row2[i])
	}
}

// @author: xftan
// @date: 2023/10/13 11:28
// @description: test write raw block with fields
func TestTaosWriteRawBlockWithFields(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_write_block_raw_fields")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_write_block_raw_fields")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database test_write_block_raw_fields")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_write_block_raw_fields"))
	// todo: TDengine not support taos_write_raw_block_with_fields with blob data
	err = exec(conn, "create table if not exists test_write_block_raw_fields.all_type (ts timestamp,"+
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
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_write_block_raw_fields.t0 using test_write_block_raw_fields.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','123456.456')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "create table test_write_block_raw_fields.t1 using test_write_block_raw_fields.all_type tags('{\"a\":2}')"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "use test_write_block_raw_fields"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select ts,c1 from test_write_block_raw_fields.t0"
	res := TaosQuery(conn, sql)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		fieldsCount := TaosNumFields(res)
		fields := TaosFetchFields(res)

		errCode = TaosWriteRawBlockWithFields(conn, blockSize, block, "t1", fields, fieldsCount)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(nil)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw_fields.t1"
	data, err := query(conn, sql)
	require.NoError(t, err)

	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	for i := 2; i < 18; i++ {
		assert.Nil(t, row1[i])
	}
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 18; i++ {
		assert.Nil(t, row2[i])
	}
}

// @author: xftan
// @date: 2023/11/17 9:39
// @description: test write raw block with reqid
func TestTaosWriteRawBlockWithReqID(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_write_block_raw_with_reqid")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_write_block_raw_with_reqid")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database test_write_block_raw_with_reqid")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_write_block_raw_with_reqid"))
	// todo: TDengine not support taos_write_raw_block_with_reqid with blob data
	err = exec(conn, "create table if not exists test_write_block_raw_with_reqid.all_type (ts timestamp,"+
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
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_write_block_raw_with_reqid.t0 using test_write_block_raw_with_reqid.all_type tags('{\"a\":1}') values"+
		"('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','123456.456')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "create table test_write_block_raw_with_reqid.t1 using test_write_block_raw_with_reqid.all_type tags('{\"a\":2}')"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "use test_write_block_raw_with_reqid"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select * from test_write_block_raw_with_reqid.t0"
	res := TaosQuery(conn, sql)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}

		errCode = TaosWriteRawBlockWithReqID(conn, blockSize, block, "t1", 1)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(nil)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw_with_reqid.t1"
	data, err := query(conn, sql)
	require.NoError(t, err)

	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	assert.Equal(t, int8(1), row1[2].(int8))
	assert.Equal(t, int16(1), row1[3].(int16))
	assert.Equal(t, int32(1), row1[4].(int32))
	assert.Equal(t, int64(1), row1[5].(int64))
	assert.Equal(t, uint8(1), row1[6].(uint8))
	assert.Equal(t, uint16(1), row1[7].(uint16))
	assert.Equal(t, uint32(1), row1[8].(uint32))
	assert.Equal(t, uint64(1), row1[9].(uint64))
	assert.Equal(t, float32(1), row1[10].(float32))
	assert.Equal(t, float64(1), row1[11].(float64))
	assert.Equal(t, "test_binary", row1[12].(string))
	assert.Equal(t, "test_nchar", row1[13].(string))
	assert.Equal(t, []byte("test_varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, row1[15].([]byte))
	assert.Equal(t, "123.4560", row1[16].(string))
	assert.Equal(t, "123456.4560", row1[17].(string))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 18; i++ {
		assert.Nil(t, row2[i])
	}
}

// @author: xftan
// @date: 2023/11/17 9:37
// @description: test write raw block with fields and reqid
func TestTaosWriteRawBlockWithFieldsWithReqID(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer TaosClose(conn)
	err = exec(conn, "drop database if exists test_write_block_raw_fields_with_reqid")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_write_block_raw_fields_with_reqid")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database test_write_block_raw_fields_with_reqid")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "test_write_block_raw_fields_with_reqid"))
	// todo: TDengine not support taos_write_raw_block_with_fields_with_reqid with blob data
	err = exec(conn, "create table if not exists test_write_block_raw_fields_with_reqid.all_type (ts timestamp,"+
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
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_write_block_raw_fields_with_reqid.t0 using test_write_block_raw_fields_with_reqid.all_type tags('{\"a\":1}') values"+
		"('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','123456.456')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "create table test_write_block_raw_fields_with_reqid.t1 using test_write_block_raw_fields_with_reqid.all_type tags('{\"a\":2}')"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "use test_write_block_raw_fields_with_reqid"
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select ts,c1 from test_write_block_raw_fields_with_reqid.t0"
	res := TaosQuery(conn, sql)
	code := TaosError(res)
	if code != 0 {
		errStr := TaosErrorStr(res)
		TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	for {
		blockSize, errCode, block := TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(res)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		fieldsCount := TaosNumFields(res)
		fields := TaosFetchFields(res)

		errCode = TaosWriteRawBlockWithFieldsWithReqID(conn, blockSize, block, "t1", fields, fieldsCount, 1)
		if errCode != int(errors.SUCCESS) {
			errStr := TaosErrorStr(nil)
			err := errors.NewError(errCode, errStr)
			t.Error(err)
			TaosFreeResult(res)
			return
		}
	}
	TaosFreeResult(res)

	sql = "select * from test_write_block_raw_fields_with_reqid.t1"
	data, err := query(conn, sql)
	require.NoError(t, err)
	assert.Equal(t, 2, len(data))
	row1 := data[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, row1[1].(bool))
	for i := 2; i < 18; i++ {
		assert.Nil(t, row1[i])
	}
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 18; i++ {
		assert.Nil(t, row2[i])
	}
}
