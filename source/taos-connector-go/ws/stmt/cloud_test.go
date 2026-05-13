package stmt

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestCloudStmt(t *testing.T) {
	db := "go_test"
	endPoint := os.Getenv("TDENGINE_CLOUD_ENDPOINT")
	token := os.Getenv("TDENGINE_CLOUD_TOKEN")
	if endPoint == "" || token == "" {
		t.Skip("TDENGINE_CLOUD_TOKEN or TDENGINE_CLOUD_ENDPOINT is not set, skip cloud test")
		return
	}
	now := time.Now()
	url := fmt.Sprintf("wss://%s?token=%s", endPoint, token)
	config := NewConfig(url, 0)
	err := config.SetConnectUser("root")
	assert.NoError(t, err)
	err = config.SetConnectPass("taosdata")
	assert.NoError(t, err)
	err = config.SetConnectDB(db)
	assert.NoError(t, err)
	err = config.SetMessageTimeout(common.DefaultMessageTimeout)
	assert.NoError(t, err)
	err = config.SetWriteWait(common.DefaultWriteWait)
	assert.NoError(t, err)
	config.SetEnableCompression(true)
	connector, err := NewConnector(config)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = connector.Close()
		assert.NoError(t, err)
	}()
	jsonStableName := fmt.Sprintf("all_json_%d", now.UnixNano())
	normalStableName := fmt.Sprintf("all_all_%d", now.UnixNano())
	createStableSqls := []string{
		fmt.Sprintf("create table if not exists %s(ts timestamp,"+
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
			"c13 nchar(20)"+
			")"+
			"tags(t json)", jsonStableName),
		fmt.Sprintf("create table if not exists %s("+
			"ts timestamp,"+
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
			"c13 nchar(20)"+
			")"+
			"tags("+
			"tts timestamp,"+
			"tc1 bool,"+
			"tc2 tinyint,"+
			"tc3 smallint,"+
			"tc4 int,"+
			"tc5 bigint,"+
			"tc6 tinyint unsigned,"+
			"tc7 smallint unsigned,"+
			"tc8 int unsigned,"+
			"tc9 bigint unsigned,"+
			"tc10 float,"+
			"tc11 double,"+
			"tc12 binary(20),"+
			"tc13 nchar(20))", normalStableName),
	}
	for _, sql := range createStableSqls {
		err = cloudDoRequest(endPoint, token, db, sql)
		if !assert.NoError(t, err, sql) {
			return
		}
	}
	defer func() {
		err = cloudDoRequest(endPoint, token, db, fmt.Sprintf("drop table if exists %s", jsonStableName))
		assert.NoError(t, err)
		err = cloudDoRequest(endPoint, token, db, fmt.Sprintf("drop table if exists %s", normalStableName))
		assert.NoError(t, err)
	}()
	{
		stmt, err := connector.Init()
		if err != nil {
			t.Error(err)
			return
		}
		err = stmt.Prepare(fmt.Sprintf("insert into ? using %s tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", jsonStableName))
		if err != nil {
			t.Error(err)
			return
		}
		subTableName := fmt.Sprintf("sub_table_%d", now.UnixNano())
		err = stmt.SetTableName(subTableName)
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
		err = stmt.Close()
		if err != nil {
			t.Error(err)
			return
		}
		result, err := cloudQuery(endPoint, token, db, fmt.Sprintf("select * from %s order by ts", jsonStableName))
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
		err = stmt.Prepare(fmt.Sprintf("insert into ? using %s tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", normalStableName))
		assert.NoError(t, err)
		subTableName1 := fmt.Sprintf("sub_table1_%d", now.UnixNano())
		subTableName2 := fmt.Sprintf("sub_table2_%d", now.UnixNano())
		err = stmt.SetTableName(subTableName1)
		if err != nil {
			t.Error(err)
			return
		}

		err = stmt.SetTableName(subTableName2)
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
		result, err := cloudQuery(endPoint, token, db, fmt.Sprintf("select * from %s order by ts", normalStableName))
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

func cloudDoRequest(endpoint, token, db string, payload string) error {
	body := strings.NewReader(payload)
	url := fmt.Sprintf("https://%s/rest/sql/%s?token=%s", endpoint, db, token)
	req, _ := http.NewRequest(http.MethodPost, url, body)
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
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}

func cloudQuery(endpoint, token, db string, payload string) (*common.TDEngineRestfulResp, error) {
	body := strings.NewReader(payload)
	url := fmt.Sprintf("https://%s/rest/sql/%s?token=%s", endpoint, db, token)
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http code: %d", resp.StatusCode)
	}
	return common.UnmarshalRestfulBody(resp.Body, 512)
}
