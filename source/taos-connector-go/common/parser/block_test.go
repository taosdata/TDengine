package parser

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test read row
func TestReadRow(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	err = exec(conn, "drop database if exists test_read_row")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists test_read_row")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database test_read_row")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists test_read_row.all_type (ts timestamp,"+
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
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into test_read_row.t0 using test_read_row.all_type tags('{\"a\":1}') values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','varbinary','point(100 100)','-123.4','1234.56','blob')('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select * from test_read_row.all_type"
	res := wrapper.TaosQuery(conn, sql)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		for i := 0; i < blockSize; i++ {
			tmp := make([]driver.Value, fileCount)
			err = ReadRow(tmp, block, blockSize, i, rh.ColTypes, precision, rh.Scales)
			assert.NoError(t, err)
			data = append(data, tmp)
		}
	}
	wrapper.TaosFreeResult(res)
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
	assert.Equal(t, []byte("varbinary"), row1[14].([]byte))
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, row1[15].([]byte))
	assert.Equal(t, "-123.4000", row1[16].(string))
	assert.Equal(t, "1234.5600", row1[17].(string))
	assert.Equal(t, []byte("blob"), row1[18].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[19].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 19; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[19].([]byte))
}

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test parse block
func TestParseBlock(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	err = exec(conn, "drop database if exists parse_block")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists parse_block")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database parse_block vgroups 1")
	require.NoError(t, err)
	err = exec(conn, "create table if not exists parse_block.all_type (ts timestamp,"+
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
		") tags (info json)")
	require.NoError(t, err)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into parse_block.t0 using parse_block.all_type tags('{\"a\":1}') "+
		"values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','POINT(100 100)',123456789.123,123.456,'blob')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	err = exec(conn, sql)
	require.NoError(t, err)

	sql = "select * from parse_block.all_type"
	res := wrapper.TaosQuery(conn, sql)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			t.Error(err)
			wrapper.TaosFreeResult(res)
			return
		}
		if blockSize == 0 {
			break
		}
		version := RawBlockGetVersion(block)
		t.Log(version)
		length := RawBlockGetLength(block)
		assert.Equal(t, int32(541), length)
		rows := RawBlockGetNumOfRows(block)
		assert.Equal(t, int32(2), rows)
		columns := RawBlockGetNumOfCols(block)
		assert.Equal(t, int32(20), columns)
		hasColumnSegment := RawBlockGetHasColumnSegment(block)
		assert.Equal(t, int32(-2147483648), hasColumnSegment)
		groupId := RawBlockGetGroupID(block)
		assert.Equal(t, uint64(0), groupId)
		infos := make([]RawBlockColInfo, columns)
		RawBlockGetColInfo(block, infos)
		assert.Equal(
			t,
			[]RawBlockColInfo{
				{
					ColType: common.TSDB_DATA_TYPE_TIMESTAMP,
					Bytes:   8,
				},
				{
					ColType: common.TSDB_DATA_TYPE_BOOL,
					Bytes:   1,
				},
				{
					ColType: common.TSDB_DATA_TYPE_TINYINT,
					Bytes:   1,
				},
				{
					ColType: common.TSDB_DATA_TYPE_SMALLINT,
					Bytes:   2,
				},
				{
					ColType: common.TSDB_DATA_TYPE_INT,
					Bytes:   4,
				},
				{
					ColType: common.TSDB_DATA_TYPE_BIGINT,
					Bytes:   8,
				},
				{
					ColType: common.TSDB_DATA_TYPE_UTINYINT,
					Bytes:   1,
				},
				{
					ColType: common.TSDB_DATA_TYPE_USMALLINT,
					Bytes:   2,
				},
				{
					ColType: common.TSDB_DATA_TYPE_UINT,
					Bytes:   4,
				},
				{
					ColType: common.TSDB_DATA_TYPE_UBIGINT,
					Bytes:   8,
				},
				{
					ColType: common.TSDB_DATA_TYPE_FLOAT,
					Bytes:   4,
				},
				{
					ColType: common.TSDB_DATA_TYPE_DOUBLE,
					Bytes:   8,
				},
				{
					ColType: common.TSDB_DATA_TYPE_BINARY,
					Bytes:   22,
				},
				{
					ColType: common.TSDB_DATA_TYPE_NCHAR,
					Bytes:   82,
				},
				{
					ColType: common.TSDB_DATA_TYPE_VARBINARY,
					Bytes:   22,
				},
				{
					ColType: common.TSDB_DATA_TYPE_GEOMETRY,
					Bytes:   102,
				},
				{
					ColType: common.TSDB_DATA_TYPE_DECIMAL,
					// scale,precision,empty,len
					Bytes: int32(binary.LittleEndian.Uint32([]byte{4, 20, 0, 16})),
				},
				{
					ColType: common.TSDB_DATA_TYPE_DECIMAL64,
					Bytes:   int32(binary.LittleEndian.Uint32([]byte{4, 10, 0, 8})),
				},
				{
					ColType: common.TSDB_DATA_TYPE_BLOB,
					// todo
					Bytes: 5,
				},
				{
					ColType: common.TSDB_DATA_TYPE_JSON,
					Bytes:   16384,
				},
			},
			infos,
		)
		d, err := ReadBlockSimple(block, precision)
		assert.NoError(t, err)
		data = append(data, d...)
	}
	wrapper.TaosFreeResult(res)
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
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, row1[15].([]byte))
	assert.Equal(t, "123456789.1230", row1[16].(string))
	assert.Equal(t, "123.4560", row1[17].(string))
	assert.Equal(t, []byte("blob"), row1[18].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[19].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 19; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[19].([]byte))
}

func Test_validColumnType(t *testing.T) {
	type args struct {
		colTypes []uint8
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "validColumnType",
			args: args{
				colTypes: []uint8{
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
					common.TSDB_DATA_TYPE_TIMESTAMP,
					common.TSDB_DATA_TYPE_DECIMAL64,
					common.TSDB_DATA_TYPE_DECIMAL,
					common.TSDB_DATA_TYPE_BINARY,
					common.TSDB_DATA_TYPE_NCHAR,
					common.TSDB_DATA_TYPE_JSON,
					common.TSDB_DATA_TYPE_VARBINARY,
					common.TSDB_DATA_TYPE_GEOMETRY,
					common.TSDB_DATA_TYPE_BLOB,
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalidColumnType",
			args: args{
				colTypes: []uint8{
					common.TSDB_DATA_TYPE_NULL,
					common.TSDB_DATA_TYPE_MEDIUMBLOB,
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "overflowColumnType",
			args: args{
				colTypes: []uint8{
					common.TSDB_DATA_TYPE_MAX,
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, validColumnType(tt.args.colTypes), fmt.Sprintf("validColumnType(%v)", tt.args.colTypes))
		})
	}
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
		return errors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(res)
	return nil
}
