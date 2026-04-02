package parser

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:18
// @description: test parse block
func TestParseBlock(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}

	defer wrapper.TaosClose(conn)
	res := wrapper.TaosQuery(conn, "drop database if exists parse_block")
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	defer func() {
		res = wrapper.TaosQuery(conn, "drop database if exists parse_block")
		code = wrapper.TaosError(res)
		if code != 0 {
			errStr := wrapper.TaosErrorStr(res)
			wrapper.TaosFreeResult(res)
			t.Error(errors.NewError(code, errStr))
			return
		}
		wrapper.TaosFreeResult(res)
	}()
	res = wrapper.TaosQuery(conn, "create database parse_block vgroups 1")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	assert.NoError(t, EnsureDBCreated("parse_block"))

	res = wrapper.TaosQuery(conn, "create table if not exists parse_block.all_type (ts timestamp,"+
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
		"c17 blob"+
		") tags (info json)")
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)
	now := time.Now()
	after1s := now.Add(time.Second)
	sql := fmt.Sprintf("insert into parse_block.t0 using parse_block.all_type tags('{\"a\":1}') "+
		"values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar','test_varbinary','POINT(100 100)',123456789.123,'\\x010203ddff')"+
		"('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)", now.Format(time.RFC3339Nano), after1s.Format(time.RFC3339Nano))
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		t.Error(errors.NewError(code, errStr))
		return
	}
	wrapper.TaosFreeResult(res)

	sql = "select * from parse_block.all_type"
	res = wrapper.TaosQuery(conn, sql)
	code = wrapper.TaosError(res)
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
		assert.Equal(t, int32(516), length)
		rows := RawBlockGetNumOfRows(block)
		assert.Equal(t, int32(2), rows)
		columns := RawBlockGetNumOfCols(block)
		assert.Equal(t, int32(19), columns)
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
					ColType: 9,
					Bytes:   8,
				},
				{
					ColType: 1,
					Bytes:   1,
				},
				{
					ColType: 2,
					Bytes:   1,
				},
				{
					ColType: 3,
					Bytes:   2,
				},
				{
					ColType: 4,
					Bytes:   4,
				},
				{
					ColType: 5,
					Bytes:   8,
				},
				{
					ColType: 11,
					Bytes:   1,
				},
				{
					ColType: 12,
					Bytes:   2,
				},
				{
					ColType: 13,
					Bytes:   4,
				},
				{
					ColType: 14,
					Bytes:   8,
				},
				{
					ColType: 6,
					Bytes:   4,
				},
				{
					ColType: 7,
					Bytes:   8,
				},
				{
					ColType: 8,
					Bytes:   22,
				},
				{
					ColType: 10,
					Bytes:   82,
				},
				{
					ColType: 16,
					Bytes:   22,
				},
				{
					ColType: 20,
					Bytes:   102,
				},
				{
					ColType: 17,
					// scale,precision,empty,len
					Bytes: int32(binary.LittleEndian.Uint32([]byte{4, 20, 0, 16})),
				},
				{
					ColType: 18,
					Bytes:   5,
				},
				{
					ColType: 15,
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
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0xdd, 0xff}, row1[17].([]byte))
	assert.Equal(t, []byte(`{"a":1}`), row1[18].([]byte))
	row2 := data[1]
	assert.Equal(t, after1s.UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 18; i++ {
		assert.Nil(t, row2[i])
	}
	assert.Equal(t, []byte(`{"a":1}`), row2[18].([]byte))
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
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalidColumnType",
			args: args{
				colTypes: []uint8{
					common.TSDB_DATA_TYPE_NULL,
					common.TSDB_DATA_TYPE_BLOB,
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

func EnsureDBCreated(dbName string) error {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		return err
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < 100; i++ {
		value, err := Query(conn, fmt.Sprintf(`select * from performance_schema.perf_trans where db = '%s'`, dbName))
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err = Query(conn, fmt.Sprintf(`select * from information_schema.ins_databases where name = '%s'`, dbName))
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

func Query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := wrapper.TaosErrorStr(res)
			return nil, errors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}
