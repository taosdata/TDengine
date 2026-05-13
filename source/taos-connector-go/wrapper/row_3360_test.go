package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/errors"
)

func TestFetchRowAllType_3360(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	db := "test_fetch_row_all_3360"

	err = exec(conn, "drop database if exists "+db)
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop database if exists "+db)
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists "+db)
	require.NoError(t, err)
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
			"c17 decimal(10,4)"+
			")"+
			"tags(t json)", db))
	require.NoError(t, err)

	err = exec(conn, fmt.Sprintf("create table if not exists %s.tb1 using %s.stb1 tags('{\"a\":1}')", db, db))
	require.NoError(t, err)
	now := time.Now()
	err = exec(conn, fmt.Sprintf("insert into %s.tb1 values('%s',true,2,3,4,5,6,7,8,9,10,11,'binary','nchar','varbinary','POINT(100 100)',123456789.123,123.456);", db, now.Format(time.RFC3339Nano)))
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
	assert.Equal(t, []byte(`{"a":1}`), result[18].([]byte))
}
