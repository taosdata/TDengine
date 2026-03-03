package stmt

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/testenv"
	"github.com/taosdata/driver-go/v3/common/testtool"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func prepareEnv(db string) error {
	var err error
	steps := []string{
		"drop database if exists " + db,
		"create database " + db,
		"create table " + db + ".all_json(ts timestamp," +
			"c1 bool," +
			"c2 tinyint," +
			"c3 smallint," +
			"c4 int," +
			"c5 bigint," +
			"c6 tinyint unsigned," +
			"c7 smallint unsigned," +
			"c8 int unsigned," +
			"c9 bigint unsigned," +
			"c10 float," +
			"c11 double," +
			"c12 binary(20)," +
			"c13 nchar(20)" +
			")" +
			"tags(t json)",
		"create table " + db + ".all_all(" +
			"ts timestamp," +
			"c1 bool," +
			"c2 tinyint," +
			"c3 smallint," +
			"c4 int," +
			"c5 bigint," +
			"c6 tinyint unsigned," +
			"c7 smallint unsigned," +
			"c8 int unsigned," +
			"c9 bigint unsigned," +
			"c10 float," +
			"c11 double," +
			"c12 binary(20)," +
			"c13 nchar(20)" +
			")" +
			"tags(" +
			"tts timestamp," +
			"tc1 bool," +
			"tc2 tinyint," +
			"tc3 smallint," +
			"tc4 int," +
			"tc5 bigint," +
			"tc6 tinyint unsigned," +
			"tc7 smallint unsigned," +
			"tc8 int unsigned," +
			"tc9 bigint unsigned," +
			"tc10 float," +
			"tc11 double," +
			"tc12 binary(20)," +
			"tc13 nchar(20))",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanEnv(db string) error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop database if exists " + db,
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func doRequest(payload string) error {
	body := strings.NewReader(payload)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http code: %d", resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	iter := client.JsonI.BorrowIterator(data)
	code := int32(0)
	desc := ""
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			code = iter.ReadInt32()
		case "desc":
			desc = iter.ReadString()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	client.JsonI.ReturnIterator(iter)
	if code != 0 {
		if code == 0x3d3 {
			time.Sleep(100 * time.Millisecond)
			return doRequest(payload)
		}
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}

// @author: xftan
// @date: 2023/10/13 11:35
// @description: test stmt over websocket
func TestStmt(t *testing.T) {
	err := prepareEnv("test_ws_stmt")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv("test_ws_stmt")
		assert.NoError(t, err)
	}()
	now := time.Now()
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB("test_ws_stmt")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(param.NewParam(1).AddJson([]byte(`{"tb":1}`)), param.NewColumnType(1).AddJson(8))
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Close()
		if err != nil {
			t.Error(err)
			return
		}
		result, err := testtool.HTTPQuery("select * from test_ws_stmt.all_json order by ts")
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, 0, result.Code, result)
		assert.Equal(t, 3, len(result.Data))
		assert.Equal(t, 15, len(result.ColTypes))
		row1 := result.Data[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, []byte(`{"tb":1}`), row1[14])
		row2 := result.Data[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, []byte(`{"tb":1}`), row2[14])
		row3 := result.Data[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, []byte(`{"tb":1}`), row3[14])
	}
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare("insert into ? using all_all tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		assert.NoError(t, err)
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}

		err = stmt.SetTableName("tb2")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(
			param.NewParam(14).
				AddTimestamp(now, 0).
				AddBool(true).
				AddTinyint(2).
				AddSmallint(2).
				AddInt(2).
				AddBigint(2).
				AddUTinyint(2).
				AddUSmallint(2).
				AddUInt(2).
				AddUBigint(2).
				AddFloat(2).
				AddDouble(2).
				AddBinary([]byte("tb2")).
				AddNchar("tb2"),
			param.NewColumnType(14).
				AddTimestamp().
				AddBool().
				AddTinyint().
				AddSmallint().
				AddInt().
				AddBigint().
				AddUTinyint().
				AddUSmallint().
				AddUInt().
				AddUBigint().
				AddFloat().
				AddDouble().
				AddBinary(0).
				AddNchar(0),
		)
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Close()
		if err != nil {
			t.Error(err)
			return
		}
		result, err := testtool.HTTPQuery("select * from test_ws_stmt.all_all order by ts")
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, 3, affected)
		assert.Equal(t, 0, result.Code, result)
		assert.Equal(t, 3, len(result.Data))
		assert.Equal(t, 28, len(result.ColTypes))
		row1 := result.Data[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row2 := result.Data[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row3 := result.Data[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, now.UnixNano()/1e6, row3[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[15])
		assert.Equal(t, int8(2), row3[16])
		assert.Equal(t, int16(2), row3[17])
		assert.Equal(t, int32(2), row3[18])
		assert.Equal(t, int64(2), row3[19])
		assert.Equal(t, uint8(2), row3[20])
		assert.Equal(t, uint16(2), row3[21])
		assert.Equal(t, uint32(2), row3[22])
		assert.Equal(t, uint64(2), row3[23])
		assert.Equal(t, float32(2), row3[24])
		assert.Equal(t, float64(2), row3[25])
		assert.Equal(t, "tb2", row3[26])
		assert.Equal(t, "tb2", row3[27])
	}
}

func TestSTMTQuery(t *testing.T) {
	err := prepareEnv("test_ws_stmt_query")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv("test_ws_stmt_query")
		assert.NoError(t, err)
	}()
	now := time.Now()
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB("test_ws_stmt_query")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_ = connector.Close()
	}()
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			_ = stmt.Close()
		}()
		err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(param.NewParam(1).AddJson([]byte(`{"tb":1}`)), param.NewColumnType(1).AddJson(0))
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Prepare("select * from all_json where ts >=? order by ts")
		assert.NoError(t, err)
		queryTime := now.Format(time.RFC3339Nano)
		params = []*param.Param{param.NewParam(1).AddBinary([]byte(queryTime))}
		paramTypes = param.NewColumnType(1).AddBinary(len(queryTime))
		err = stmt.BindParam(params, paramTypes)
		assert.NoError(t, err)
		err = stmt.AddBatch()
		assert.NoError(t, err)
		err = stmt.Exec()
		assert.NoError(t, err)
		rows, err := stmt.UseResult()
		assert.NoError(t, err)
		columns := rows.Columns()
		assert.Equal(t, 15, len(columns))
		expectColumns := []string{
			"ts",
			"c1",
			"c2",
			"c3",
			"c4",
			"c5",
			"c6",
			"c7",
			"c8",
			"c9",
			"c10",
			"c11",
			"c12",
			"c13",
			"t",
		}
		for i := 0; i < 14; i++ {
			assert.Equal(t, columns[i], expectColumns[i])
			rows.ColumnTypeDatabaseTypeName(i)
			rows.ColumnTypeLength(i)
			rows.ColumnTypeScanType(i)
		}
		var result [][]driver.Value
		for {
			values := make([]driver.Value, 15)
			err = rows.Next(values)
			if err != nil {
				if err == io.EOF {
					_ = rows.Close()
					break
				}
				assert.NoError(t, err)
			}
			result = append(result, values)
		}
		assert.Equal(t, 3, len(result))
		row1 := result[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, []byte(`{"tb":1}`), row1[14])
		row2 := result[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, []byte(`{"tb":1}`), row2[14])
		row3 := result[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, []byte(`{"tb":1}`), row3[14])
	}
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		defer func(stmt *Stmt) {
			err := stmt.Close()
			if err != nil {
				t.Error(err)
			}
		}(stmt)
		err = stmt.Prepare("insert into ? using all_all tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		assert.NoError(t, err)
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}

		err = stmt.SetTableName("tb2")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(
			param.NewParam(14).
				AddTimestamp(now, 0).
				AddBool(true).
				AddTinyint(2).
				AddSmallint(2).
				AddInt(2).
				AddBigint(2).
				AddUTinyint(2).
				AddUSmallint(2).
				AddUInt(2).
				AddUBigint(2).
				AddFloat(2).
				AddDouble(2).
				AddBinary([]byte("tb2")).
				AddNchar("tb2"),
			param.NewColumnType(14).
				AddTimestamp().
				AddBool().
				AddTinyint().
				AddSmallint().
				AddInt().
				AddBigint().
				AddUTinyint().
				AddUSmallint().
				AddUInt().
				AddUBigint().
				AddFloat().
				AddDouble().
				AddBinary(0).
				AddNchar(0),
		)
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Prepare("select * from all_all where ts >=? order by ts")
		assert.NoError(t, err)
		queryTime := now.Format(time.RFC3339Nano)
		params = []*param.Param{param.NewParam(1).AddBinary([]byte(queryTime))}
		paramTypes = param.NewColumnType(1).AddBinary(len(queryTime))
		err = stmt.BindParam(params, paramTypes)
		assert.NoError(t, err)
		err = stmt.AddBatch()
		assert.NoError(t, err)
		err = stmt.Exec()
		assert.NoError(t, err)
		rows, err := stmt.UseResult()
		assert.NoError(t, err)
		columns := rows.Columns()
		assert.Equal(t, 28, len(columns))
		var result [][]driver.Value
		for {
			values := make([]driver.Value, 28)
			err = rows.Next(values)
			if err != nil {
				if err == io.EOF {
					_ = rows.Close()
					break
				}
				assert.NoError(t, err)
			}
			result = append(result, values)
		}
		assert.Equal(t, 3, len(result))
		row1 := result[0]
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row2 := result[1]
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, now.UnixNano()/1e6, row1[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[15])
		assert.Equal(t, int8(2), row1[16])
		assert.Equal(t, int16(2), row1[17])
		assert.Equal(t, int32(2), row1[18])
		assert.Equal(t, int64(2), row1[19])
		assert.Equal(t, uint8(2), row1[20])
		assert.Equal(t, uint16(2), row1[21])
		assert.Equal(t, uint32(2), row1[22])
		assert.Equal(t, uint64(2), row1[23])
		assert.Equal(t, float32(2), row1[24])
		assert.Equal(t, float64(2), row1[25])
		assert.Equal(t, "tb2", row1[26])
		assert.Equal(t, "tb2", row1[27])
		row3 := result[2]
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, now.UnixNano()/1e6, row3[14].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[15])
		assert.Equal(t, int8(2), row3[16])
		assert.Equal(t, int16(2), row3[17])
		assert.Equal(t, int32(2), row3[18])
		assert.Equal(t, int64(2), row3[19])
		assert.Equal(t, uint8(2), row3[20])
		assert.Equal(t, uint16(2), row3[21])
		assert.Equal(t, uint32(2), row3[22])
		assert.Equal(t, uint64(2), row3[23])
		assert.Equal(t, float32(2), row3[24])
		assert.Equal(t, float64(2), row3[25])
		assert.Equal(t, "tb2", row3[26])
		assert.Equal(t, "tb2", row3[27])
	}
}

func TestStmtQueryDecimal(t *testing.T) {
	err := prepareEnv("test_ws_stmt_query_decimal")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv("test_ws_stmt_query_decimal")
		assert.NoError(t, err)
	}()
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB("test_ws_stmt_query_decimal")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_ = connector.Close()
	}()
	now := time.Now().UTC().Round(time.Millisecond)
	err = doRequest("create table test_ws_stmt_query_decimal.tb1(ts timestamp, c1 decimal(10, 4), c2 decimal(20, 4))")
	assert.NoError(t, err)
	err = doRequest(fmt.Sprintf("insert into test_ws_stmt_query_decimal.tb1 values('%s', 1.23, 2.34)", now.Format(time.RFC3339Nano)))
	stmt, err := connector.Init()
	assert.NoError(t, err)
	err = stmt.Prepare("select * from tb1 where ts = ?")
	assert.NoError(t, err)
	queryTime := []byte(now.Format(time.RFC3339Nano))
	params := []*param.Param{param.NewParam(1).AddBinary(queryTime)}
	paramTypes := param.NewColumnType(1).AddBinary(len(queryTime))
	err = stmt.BindParam(params, paramTypes)
	assert.NoError(t, err)
	err = stmt.AddBatch()
	assert.NoError(t, err)
	err = stmt.Exec()
	assert.NoError(t, err)
	rows, err := stmt.UseResult()
	assert.NoError(t, err)
	columns := rows.Columns()
	assert.Equal(t, 3, len(columns))
	precision, scale, ok := rows.ColumnTypePrecisionScale(0)
	assert.False(t, ok)
	assert.Equal(t, int64(0), precision)
	assert.Equal(t, int64(0), scale)
	precision, scale, ok = rows.ColumnTypePrecisionScale(1)
	assert.True(t, ok)
	assert.Equal(t, int64(10), precision)
	assert.Equal(t, int64(4), scale)
	precision, scale, ok = rows.ColumnTypePrecisionScale(2)
	assert.True(t, ok)
	assert.Equal(t, int64(20), precision)
	assert.Equal(t, int64(4), scale)
	var result [][]driver.Value
	for {
		values := make([]driver.Value, 3)
		err = rows.Next(values)
		if err != nil {
			if err == io.EOF {
				_ = rows.Close()
				break
			}
			assert.NoError(t, err)
		}
		result = append(result, values)
	}
	assert.Equal(t, 1, len(result))
	row1 := result[0]
	assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, "1.2300", row1[1].(string))
	assert.Equal(t, "2.3400", row1[2].(string))
}

func TestStmtClose(t *testing.T) {
	err := prepareEnv("test_ws_stmt_close")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv("test_ws_stmt_close")
		assert.NoError(t, err)
	}()
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB("test_ws_stmt_close")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		_ = connector.Close()
	}()
	err = doRequest("create table test_ws_stmt_close.tb1(ts timestamp, c1 int)")
	assert.NoError(t, err)
	err = doRequest("insert into test_ws_stmt_close.tb1 values(now, 1)")
	assert.NoError(t, err)
	stmt, err := connector.Init()
	assert.NoError(t, err)
	err = connector.Close()
	assert.NoError(t, err)
	err = stmt.Prepare("select * from tb1 where c1 = ?")
	assert.Equal(t, client.ClosedError, err)
	stmtNew, err := connector.Init()
	assert.Equal(t, ErrConnIsClosed, err)
	assert.Nil(t, stmtNew)
	err = stmt.BindParam([]*param.Param{param.NewParam(1).AddInt(1)}, param.NewColumnType(1).AddInt())
	assert.Equal(t, client.ClosedError, err)
}

func newTaosadapter(port string) *exec.Cmd {
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"

	}
	return exec.Command(command, "--port", port, "--log.level", "debug")
}

func startTaosadapter(cmd *exec.Cmd, port string) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		return nil
	}
	return errors.New("taosadapter start failed")
}

func stopTaosadapter(cmd *exec.Cmd, port string) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	_, _ = cmd.Process.Wait()
	cmd.Process = nil
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			return
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		continue
	}
	panic("taosadapter stop failed")
}

func TestSTMTReconnect(t *testing.T) {
	port := "36042"
	cmd := newTaosadapter(port)
	err := startTaosadapter(cmd, port)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopTaosadapter(cmd, port)
	}()
	config := NewConfig("ws://127.0.0.1:"+port, 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(3 * time.Second)
	assert.NoError(t, err)
	err = config.SetWriteWait(3 * time.Second)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetErrorHandler(func(connector *Connector, err error) {
		t.Log(err)
	})
	config.SetCloseHandler(func() {
		t.Log("stmt websocket closed")
	})
	config.SetAutoReconnect(true)
	config.SetReconnectRetryCount(10)
	config.SetReconnectIntervalMs(2000)
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	stmt, err := connector.Init()
	assert.NoError(t, err)
	err = stmt.Close()
	assert.NoError(t, err)
	stopTaosadapter(cmd, port)
	startChan := make(chan struct{})
	go func() {
		time.Sleep(time.Second * 3)
		cmd = newTaosadapter(port)
		err = startTaosadapter(cmd, port)
		startChan <- struct{}{}
		if err != nil {
			t.Error(err)
			return
		}
	}()
	stmt, err = connector.Init()
	assert.Error(t, err)
	assert.Nil(t, stmt)
	<-startChan
	time.Sleep(time.Second)
	stmt, err = connector.Init()
	assert.NoError(t, err)
	stopTaosadapter(cmd, port)
	cmd = newTaosadapter(port)
	err = startTaosadapter(cmd, port)
	assert.NoError(t, err)
	err = doRequest("create database if not exists test_ws_stmt_reconnect")
	assert.NoError(t, err)
	err = doRequest("create table if not exists test_ws_stmt_reconnect.tb1(ts timestamp, c1 int)")
	assert.NoError(t, err)
	err = doRequest("insert into test_ws_stmt_reconnect.tb1 values(now, 1)")
	assert.NoError(t, err)
	err = stmt.Prepare("select * from tb1 where c1 = ?")
	assert.Error(t, err)
	err = stmt.Close()
	assert.NoError(t, err)
	stmtNew, err := connector.Init()
	assert.NoError(t, err)
	err = stmtNew.Prepare("select * from tb1 where c1 = ?")
	assert.NoError(t, err)
	err = stmtNew.Close()
	assert.NoError(t, err)
}

func TestTimezone(t *testing.T) {
	var dbname = "test_ws_stmt_timezone"
	err := prepareEnv(dbname)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv(dbname)
		assert.NoError(t, err)
	}()
	now := time.Now().Round(time.Millisecond)
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB(dbname)
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	err = config.SetConnectionTimezone("Europe/Paris")
	require.NoError(t, err)
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare("insert into ? using all_json tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTableName("tb1")
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.SetTags(param.NewParam(1).AddJson([]byte(`{"tb":1}`)), param.NewColumnType(1).AddJson(8))
		if err != nil {
			t.Error(err)
			return
		}
		params := []*param.Param{
			param.NewParam(3).AddTimestamp(now, 0).AddTimestamp(now.Add(time.Second), 0).AddTimestamp(now.Add(time.Second*2), 0),
			param.NewParam(3).AddBool(true).AddNull().AddBool(true),
			param.NewParam(3).AddTinyint(1).AddNull().AddTinyint(1),
			param.NewParam(3).AddSmallint(1).AddNull().AddSmallint(1),
			param.NewParam(3).AddInt(1).AddNull().AddInt(1),
			param.NewParam(3).AddBigint(1).AddNull().AddBigint(1),
			param.NewParam(3).AddUTinyint(1).AddNull().AddUTinyint(1),
			param.NewParam(3).AddUSmallint(1).AddNull().AddUSmallint(1),
			param.NewParam(3).AddUInt(1).AddNull().AddUInt(1),
			param.NewParam(3).AddUBigint(1).AddNull().AddUBigint(1),
			param.NewParam(3).AddFloat(1).AddNull().AddFloat(1),
			param.NewParam(3).AddDouble(1).AddNull().AddDouble(1),
			param.NewParam(3).AddBinary([]byte("test_binary")).AddNull().AddBinary([]byte("test_binary")),
			param.NewParam(3).AddNchar("test_nchar").AddNull().AddNchar("test_nchar"),
		}
		paramTypes := param.NewColumnType(14).
			AddTimestamp().
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddFloat().
			AddDouble().
			AddBinary(0).
			AddNchar(0)
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.AddBatch()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Exec()
		if err != nil {
			t.Error(err)
			return
		}
		affected := stmt.GetAffectedRows()
		if !assert.Equal(t, 3, affected) {
			return
		}
		err = stmt.Prepare("select * from all_json where ts >=? order by ts")
		assert.NoError(t, err)
		parisLoc, err := time.LoadLocation("Europe/Paris")
		require.NoError(t, err)
		queryTime := now.In(parisLoc).Format(time.RFC3339Nano)
		params = []*param.Param{param.NewParam(1).AddBinary([]byte(queryTime))}
		paramTypes = param.NewColumnType(1).AddBinary(len(queryTime))
		err = stmt.BindParam(params, paramTypes)
		assert.NoError(t, err)
		err = stmt.AddBatch()
		assert.NoError(t, err)
		err = stmt.Exec()
		assert.NoError(t, err)
		rows, err := stmt.UseResult()
		assert.NoError(t, err)
		columns := rows.Columns()
		assert.Equal(t, 15, len(columns))
		expectColumns := []string{
			"ts",
			"c1",
			"c2",
			"c3",
			"c4",
			"c5",
			"c6",
			"c7",
			"c8",
			"c9",
			"c10",
			"c11",
			"c12",
			"c13",
			"t",
		}
		for i := 0; i < 14; i++ {
			assert.Equal(t, columns[i], expectColumns[i])
			rows.ColumnTypeDatabaseTypeName(i)
			rows.ColumnTypeLength(i)
			rows.ColumnTypeScanType(i)
		}
		var result [][]driver.Value
		for {
			values := make([]driver.Value, 15)
			err = rows.Next(values)
			if err != nil {
				if err == io.EOF {
					_ = rows.Close()
					break
				}
				assert.NoError(t, err)
			}
			result = append(result, values)
		}
		assert.Equal(t, 3, len(result))
		row1 := result[0]
		assert.Equal(t, parisLoc, row1[0].(time.Time).Location())
		assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row1[1])
		assert.Equal(t, int8(1), row1[2])
		assert.Equal(t, int16(1), row1[3])
		assert.Equal(t, int32(1), row1[4])
		assert.Equal(t, int64(1), row1[5])
		assert.Equal(t, uint8(1), row1[6])
		assert.Equal(t, uint16(1), row1[7])
		assert.Equal(t, uint32(1), row1[8])
		assert.Equal(t, uint64(1), row1[9])
		assert.Equal(t, float32(1), row1[10])
		assert.Equal(t, float64(1), row1[11])
		assert.Equal(t, "test_binary", row1[12])
		assert.Equal(t, "test_nchar", row1[13])
		assert.Equal(t, []byte(`{"tb":1}`), row1[14])
		row2 := result[1]
		assert.Equal(t, parisLoc, row2[0].(time.Time).Location())
		assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, row2[0].(time.Time).UnixNano()/1e6)
		for i := 1; i < 14; i++ {
			assert.Nil(t, row2[i])
		}
		assert.Equal(t, []byte(`{"tb":1}`), row2[14])
		row3 := result[2]
		assert.Equal(t, parisLoc, row3[0].(time.Time).Location())
		assert.Equal(t, now.Add(time.Second*2).UnixNano()/1e6, row3[0].(time.Time).UnixNano()/1e6)
		assert.Equal(t, true, row3[1])
		assert.Equal(t, int8(1), row3[2])
		assert.Equal(t, int16(1), row3[3])
		assert.Equal(t, int32(1), row3[4])
		assert.Equal(t, int64(1), row3[5])
		assert.Equal(t, uint8(1), row3[6])
		assert.Equal(t, uint16(1), row3[7])
		assert.Equal(t, uint32(1), row3[8])
		assert.Equal(t, uint64(1), row3[9])
		assert.Equal(t, float32(1), row3[10])
		assert.Equal(t, float64(1), row3[11])
		assert.Equal(t, "test_binary", row3[12])
		assert.Equal(t, "test_nchar", row3[13])
		assert.Equal(t, []byte(`{"tb":1}`), row3[14])
	}
}

func TestConnectorInfo(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err := config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	connector, err := NewConnector(config)
	require.NoError(t, err)
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()

	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 10*time.Second, 500*time.Millisecond)
}

func TestTotpCode(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for community edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	seed := "Z7Xxoy5E8h9IuVIpTH684cFSzRNVVzgc"
	err := doRequest(fmt.Sprintf("create user totp_user_stmt pass 'totp_pass_1' TOTPSEED '%s'", seed))
	require.NoError(t, err)
	defer func() {
		err = doRequest("drop user totp_user_stmt")
		require.NoError(t, err)
	}()
	secret := common.GenerateTOTPSecret([]byte(seed))
	code := common.GenerateTOTPCode(secret, uint64(time.Now().Unix()/30), 6)
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetConnectUser("totp_user_stmt")
	assert.NoError(t, err)
	err = config.SetConnectPass("totp_pass_1")
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetTotpCode(strconv.Itoa(code))
	connector, err := NewConnector(config)
	require.NoError(t, err)
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s' and `user` = 'totp_user_stmt'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
}

func TestBearerToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for community edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	result, err := testtool.HTTPQuery("create token test_token_stmt_ws from user root")
	require.NoError(t, err)
	token := result.Data[0][0].(string)
	defer func() {
		err = doRequest("drop token test_token_stmt_ws")
		require.NoError(t, err)
	}()
	config := NewConfig("ws://127.0.0.1:6041", 0)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	config.SetBearerToken(token)
	connector, err := NewConnector(config)
	require.NoError(t, err)
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s' and `user` = 'root'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
}
