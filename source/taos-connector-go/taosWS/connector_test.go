package taosWS

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/testenv"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
)

func generateCreateTableSql(db string, withJson bool) string {
	createSql := fmt.Sprintf("create table if not exists %s.alltype(ts timestamp,"+
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
		"c14 varbinary(100),"+
		"c15 geometry(100),"+
		"c16 decimal(8,4),"+
		"c17 decimal(20,4),"+
		"c18 blob"+
		")",
		db)
	if withJson {
		createSql += " tags(t json)"
	}
	return createSql
}

func generateValues() (value []interface{}, scanValue []interface{}, insertSql string) {
	rand.Seed(time.Now().UnixNano())
	v1 := true
	v2 := int8(rand.Int())
	v3 := int16(rand.Int())
	v4 := rand.Int31()
	v5 := int64(rand.Int31())
	v6 := uint8(rand.Uint32())
	v7 := uint16(rand.Uint32())
	v8 := rand.Uint32()
	v9 := uint64(rand.Uint32())
	v10 := rand.Float32()
	v11 := rand.Float64()
	v12 := "test_binary"
	v13 := "test_nchar"
	v14 := []byte("test_varbinary")
	v15 := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}
	v16 := "123.4560"
	v17 := "-123456789.1234"
	v18 := []byte("blob")
	ts := time.Now().Round(time.Millisecond)
	var (
		cts time.Time
		c1  bool
		c2  int8
		c3  int16
		c4  int32
		c5  int64
		c6  uint8
		c7  uint16
		c8  uint32
		c9  uint64
		c10 float32
		c11 float64
		c12 string
		c13 string
		c14 []byte
		c15 []byte
		c16 string
		c17 string
		c18 []byte
	)
	return []interface{}{
			ts, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18,
		}, []interface{}{cts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18},
		fmt.Sprintf(`values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','-123456789.1234','blob')`, ts.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
}

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test all type query
func TestAllTypeQuery(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	database := "ws_test"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test null value
func TestAllTypeQueryNull(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	database := "ws_test_null"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generateValues()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') values('%s'%s)`, database, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String()))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	var tt types.RawMessage
	values[len(colValues)] = &tt
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), *(values[len(values)-1]).(*types.RawMessage))
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test compression
func TestAllTypeQueryCompression(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	database := "ws_test_compression"
	db, err := sql.Open("taosWS", dataSourceNameWithCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test all type query without json
func TestAllTypeQueryWithoutJson(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	database := "ws_test_without_json"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.alltype %s`, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	dest := make([]interface{}, len(scanValues))
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test all type query with null without json
func TestAllTypeQueryNullWithoutJson(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	database := "ws_test_without_json_null"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generateValues()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	insertSql := fmt.Sprintf(`insert into %s.alltype values('%s'%s)`, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String())
	_, err = exec(db, insertSql)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test query
func TestBatch(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		sql     string
		isQuery bool
	}{
		{
			name: "drop db",
			sql:  "drop database if exists test_batch",
		},
		{
			name: "create db",
			sql:  "create database test_batch",
		},
		{
			name: "use db",
			sql:  "use test_batch",
		},
		{
			name: "create table",
			sql:  "create table test(ts timestamp,v int)",
		},
		{
			name: "insert 1",
			sql:  fmt.Sprintf("insert into test values ('%s',1)", now.Format(time.RFC3339Nano)),
		},
		{
			name: "insert 2",
			sql:  fmt.Sprintf("insert into test values ('%s',2)", now.Add(time.Second).Format(time.RFC3339Nano)),
		},
		{
			name:    "query all",
			sql:     "select * from test order by ts",
			isQuery: true,
		},
		{
			name: "drop database",
			sql:  "drop database if exists test_batch",
		},
	}
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	//err = db.Ping()
	//if err != nil {
	//	t.Fatal(err)
	//}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isQuery {
				result, err := db.Query(tt.sql)
				assert.NoError(t, err)
				var check [][]interface{}
				for result.Next() {
					var ts time.Time
					var v int
					err := result.Scan(&ts, &v)
					assert.NoError(t, err)
					check = append(check, []interface{}{ts, v})
				}
				assert.Equal(t, 2, len(check))
				assert.Equal(t, now.UnixNano()/1e6, check[0][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, now.Add(time.Second).UnixNano()/1e6, check[1][0].(time.Time).UnixNano()/1e6)
				assert.Equal(t, int(1), check[0][1].(int))
				assert.Equal(t, int(2), check[1][1].(int))
			} else {
				_, err := exec(db, tt.sql)
				assert.NoError(t, err)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	conn := connector{
		cfg: &Config{},
	}
	db, err := conn.Connect(context.Background())
	assert.NoError(t, err)
	err = db.Close()
	assert.NoError(t, err)
	driver := conn.Driver()
	assert.Equal(t, &TDengineDriver{}, driver)
}

func TestConnectTotp(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for non-enterprise edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	rootConn, err := sql.Open("taosWS", dataSourceName)
	require.NoError(t, err)
	defer func() {
		err = rootConn.Close()
		assert.NoError(t, err)
	}()
	err = rootConn.Ping()
	require.NoError(t, err)
	seed := "Z7Xxoy5E8h9IuVIpTH684cFSzRNVVzgc"
	_, err = exec(rootConn, fmt.Sprintf("create user totp_user pass 'totp_pass_1' TOTPSEED '%s'", seed))
	require.NoError(t, err)
	defer func() {
		_, err = exec(rootConn, "drop user totp_user")
		assert.NoError(t, err)
	}()
	secret := common.GenerateTOTPSecret([]byte(seed))
	code := common.GenerateTOTPCode(secret, uint64(time.Now().Unix()/30), 6)
	totpSource := fmt.Sprintf("totp_user:totp_pass_1@ws(%s:%d)/?totpCode=%d", host, port, code)
	db, err := sql.Open("taosWS", totpSource)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	require.NoError(t, err)
	rows, err := db.Query("select 1")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()
	for rows.Next() {
		var v int
		err := rows.Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 1, v)
	}
}

func TestConnectToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("Skip totp test for non-enterprise edition")
	}
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	rootConn, err := sql.Open("taosWS", dataSourceName)
	require.NoError(t, err)
	defer func() {
		err = rootConn.Close()
		assert.NoError(t, err)
	}()
	err = rootConn.Ping()
	require.NoError(t, err)
	rows, err := rootConn.Query("create token go_ws_test_token from user root")
	require.NoError(t, err)
	defer func() {
		_, err = exec(rootConn, "drop token go_ws_test_token")
		assert.NoError(t, err)
	}()
	var token string
	for rows.Next() {
		err = rows.Scan(&token)
		require.NoError(t, err)
	}
	require.NotEmpty(t, token)
	tokenSource := fmt.Sprintf("@ws(%s:%d)/?bearerToken=%s", host, port, token)
	db, err := sql.Open("taosWS", tokenSource)
	require.NoError(t, err)
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	require.NoError(t, err)
	rows, err = db.Query("select 1")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, rows.Close())
	}()
	for rows.Next() {
		var v int
		err := rows.Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, 1, v)
	}
}

func TestConnectorInfo(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	rootConn, err := sql.Open("taosWS", dataSourceName)
	require.NoError(t, err)
	defer func() {
		err = rootConn.Close()
		assert.NoError(t, err)
	}()
	err = rootConn.Ping()
	require.NoError(t, err)
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s'", app, connectorInfo)
	t.Log(checkSql)
	assert.Eventually(t, func() bool {
		rows, err := rootConn.Query(checkSql)
		if err != nil {
			return false
		}
		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				return false
			}
		}
		return count > 0
	}, 5*time.Second, 500*time.Millisecond)
	require.NoError(t, err)
}

func TestTimezone(t *testing.T) {
	parisConn, err := sql.Open("taosWS", dataSourceNameWithParisTimezone)
	if err != nil {
		t.Fatal(err)
	}
	shanghaiConn, err := sql.Open("taosWS", dataSourceNameWithShanghaiTimezone)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = parisConn.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = shanghaiConn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = parisConn.Ping()
	if err != nil {
		t.Fatal(err)
	}
	err = shanghaiConn.Ping()
	if err != nil {
		t.Fatal(err)
	}
	database := "ws_test_timezone"
	defer func() {
		_, err = exec(parisConn, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(parisConn, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(parisConn, fmt.Sprintf("create table if not exists %s.ctb(ts timestamp,v int)", database))
	require.NoError(t, err)
	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	now := time.Now().Round(time.Millisecond)
	shanghaiNow := now.In(shanghaiTimezone)
	shanghaiTime := shanghaiNow.Format("2006-01-02 15:04:05.000")
	parisNow := now.In(parisTimezone)
	parisTime := parisNow.Format("2006-01-02 15:04:05.000")
	t.Log(shanghaiTime)
	t.Log(parisTime)
	t.Log(now)
	t.Log(shanghaiNow)
	t.Log(parisNow)
	// insert with shanghai timezone
	insertSql := fmt.Sprintf("insert into %s.ctb values ('%s',1)", database, shanghaiTime)
	t.Log(insertSql)
	_, err = exec(shanghaiConn, insertSql)
	require.NoError(t, err)
	// query with paris timezone
	querySql := fmt.Sprintf("select * from %s.ctb where ts = '%s'", database, parisTime)
	t.Log(querySql)
	rows, err := parisConn.Query(querySql)
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var ts time.Time
		var v int
		err := rows.Scan(&ts, &v)
		require.NoError(t, err)
		t.Log(ts)
		assert.NotEqual(t, ts, now)
		assert.Equal(t, parisTimezone, ts.Location())
		assert.Equal(t, shanghaiNow.UnixNano()/1e6, ts.UnixNano()/1e6)
		assert.Equal(t, 1, v)
		count += 1
	}
	assert.Equal(t, 1, count)
}

func exec(db *sql.DB, query string, args ...interface{}) (driver.Result, error) {
	result, err := db.Exec(query, args...)
	if err != nil {
		var taosErr *taosError.TaosError
		if errors.As(err, &taosErr) && taosErr.Code == 0x3d3 {
			time.Sleep(100 * time.Millisecond)
			return exec(db, query, args...)
		}
		return nil, err
	}
	return result, nil
}
