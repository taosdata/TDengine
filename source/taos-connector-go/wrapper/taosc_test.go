package wrapper

import (
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_options
func TestTaosOptions(t *testing.T) {
	type args struct {
		option int
		value  string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test_options",
			args: args{
				option: common.TSDB_OPTION_CONFIGDIR,
				value:  "/etc/taos",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaosOptions(tt.args.option, tt.args.value); got != tt.want {
				t.Errorf("TaosOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

type result struct {
	res unsafe.Pointer
	n   int
}

type TestCaller struct {
	QueryResult chan *result
	FetchResult chan *result
}

func NewTestCaller() *TestCaller {
	return &TestCaller{
		QueryResult: make(chan *result),
		FetchResult: make(chan *result),
	}
}

func (t *TestCaller) QueryCall(res unsafe.Pointer, code int) {
	t.QueryResult <- &result{
		res: res,
		n:   code,
	}
}

func (t *TestCaller) FetchCall(res unsafe.Pointer, numOfRows int) {
	t.FetchResult <- &result{
		res: res,
		n:   numOfRows,
	}
}

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_query_a
func TestTaosQueryA(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	var caller = NewTestCaller()
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		caller      *TestCaller
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				sql:         "show databases",
				caller:      caller,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := cgo.NewHandle(tt.args.caller)
			go TaosQueryA(tt.args.taosConnect, tt.args.sql, p)
			r := <-tt.args.caller.QueryResult
			t.Log("query finish")
			count := TaosNumFields(r.res)
			rowsHeader, err := ReadColumn(r.res, count)
			precision := TaosResultPrecision(r.res)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("%#v", rowsHeader)
			if r.n != 0 {
				t.Error("query result", r.n)
				return
			}
			res := r.res
			for {
				go TaosFetchRowsA(res, p)
				r = <-tt.args.caller.FetchResult
				if r.n == 0 {
					t.Log("success")
					TaosFreeResult(r.res)
					break
				} else {
					res = r.res
					for i := 0; i < r.n; i++ {
						values := make([]driver.Value, len(rowsHeader.ColNames))
						row := TaosFetchRow(res)
						lengths := FetchLengths(res, len(rowsHeader.ColNames))
						for j := range rowsHeader.ColTypes {
							if row == nil {
								t.Error(io.EOF)
								return
							}
							values[j] = FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision)
						}
					}
					t.Log("fetch rows a", r.n)
				}
			}
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:31
// @description: test taos error
func TestError(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	res := TaosQuery(conn, "asd")
	code := TaosError(res)
	assert.NotEqual(t, code, 0)
	errStr := TaosErrorStr(res)
	assert.NotEmpty(t, errStr)
}

// @author: xftan
// @date: 2023/10/13 11:31
// @description: test affected rows
func TestAffectedRows(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists affected_rows_test")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists affected_rows_test")
	require.NoError(t, err)
	err = exec(conn, "create table if not exists affected_rows_test.t0(ts timestamp,v int)")
	require.NoError(t, err)
	res := TaosQuery(conn, "insert into affected_rows_test.t0 values(now,1)")
	code := TaosError(res)
	if code != 0 {
		t.Error(errors.NewError(code, TaosErrorStr(res)))
		TaosFreeResult(res)
		return
	}
	affected := TaosAffectedRows(res)
	TaosFreeResult(res)
	assert.Equal(t, 1, affected)
}

// @author: xftan
// @date: 2022/1/27 17:29
// @description: test taos_reset_current_db
func TestTaosResetCurrentDB(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = exec(tt.args.taosConnect, "create database if not exists log")
			if err != nil {
				t.Error(err)
				return
			}
			TaosSelectDB(tt.args.taosConnect, "log")
			result := TaosQuery(tt.args.taosConnect, "select database()")
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			row := TaosFetchRow(result)
			lengths := FetchLengths(result, 1)
			currentDB := FetchRow(row, 0, 10, lengths[0])
			assert.Equal(t, "log", currentDB)
			TaosFreeResult(result)
			TaosResetCurrentDB(tt.args.taosConnect)
			result = TaosQuery(tt.args.taosConnect, "select database()")
			code = TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			row = TaosFetchRow(result)
			lengths = FetchLengths(result, 1)
			currentDB = FetchRow(row, 0, 10, lengths[0])
			assert.Nil(t, currentDB)
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:30
// @description: test taos_validate_sql
func TestTaosValidateSql(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "valid",
			args: args{
				taosConnect: conn,
				sql:         "show grants",
			},
			want: 0,
		},
		{
			name: "TSC_SQL_SYNTAX_ERROR",
			args: args{
				taosConnect: conn,
				sql:         "slect 1",
			},
			want: 9728,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaosValidateSql(tt.args.taosConnect, tt.args.sql); got&0xffff != tt.want {
				t.Errorf("TaosValidateSql() = %v, want %v", got&0xffff, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:30
// @description: test taos_is_update_query
func TestTaosIsUpdateQuery(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	tests := []struct {
		name string
		want bool
	}{
		{
			name: "create database if not exists is_update",
			want: true,
		},
		{
			name: "drop database if exists is_update",
			want: true,
		},
		{
			name: "show log.stables",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TaosQuery(conn, tt.name)
			defer TaosFreeResult(result)
			if got := TaosIsUpdateQuery(result); got != tt.want {
				t.Errorf("TaosIsUpdateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:30
// @description: taos async raw block
func TestTaosResultBlock(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	var caller = NewTestCaller()
	type args struct {
		taosConnect unsafe.Pointer
		sql         string
		caller      *TestCaller
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				taosConnect: conn,
				sql:         "show users",
				caller:      caller,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := cgo.NewHandle(tt.args.caller)
			go TaosQueryA(tt.args.taosConnect, tt.args.sql, p)
			r := <-tt.args.caller.QueryResult
			t.Log("query finish")
			count := TaosNumFields(r.res)
			rowsHeader, err := ReadColumn(r.res, count)
			if err != nil {
				t.Error(err)
				return
			}
			//t.Logf("%#v", rowsHeader)
			if r.n != 0 {
				t.Error("query result", r.n)
				return
			}
			res := r.res
			precision := TaosResultPrecision(res)
			for {
				go TaosFetchRawBlockA(res, p)
				r = <-tt.args.caller.FetchResult
				if r.n == 0 {
					t.Log("success")
					TaosFreeResult(r.res)
					break
				} else {
					res = r.res
					block := TaosGetRawBlock(res)
					assert.NotNil(t, block)
					values, err := parser.ReadBlock(block, r.n, rowsHeader.ColTypes, precision)
					assert.NoError(t, err)
					_ = values
					t.Log(values)
				}
			}
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:31
// @description: test taos_get_client_info
func TestTaosGetClientInfo(t *testing.T) {
	s := TaosGetClientInfo()
	assert.NotEmpty(t, s)
}

// @author: xftan
// @date: 2023/10/13 11:31
// @description: test taos_load_table_info
func TestTaosLoadTableInfo(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	err = exec(conn, "drop database if exists info1")
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = exec(conn, "drop database if exists info1")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database info1")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table info1.t(ts timestamp,v int)")
	if err != nil {
		t.Error(err)
		return
	}
	code := TaosLoadTableInfo(conn, []string{"info1.t"})
	if code != 0 {
		errStr := TaosErrorStr(nil)
		t.Error(errors.NewError(code, errStr))
		return
	}

}

// @author: xftan
// @date: 2023/10/13 11:32
// @description: test taos_get_table_vgId
func TestTaosGetTableVgID(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer TaosClose(conn)
	dbName := "table_vg_id_test"

	_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	defer func() {
		_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	}()
	if err = exec(conn, fmt.Sprintf("create database %s", dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create stable %s.meters (ts timestamp, current float, voltage int, phase float) "+
		"tags (location binary(64), groupId int)", dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create table %s.d0 using %s.meters tags ('California.SanFrancisco', 1)", dbName, dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create table %s.d1 using %s.meters tags ('California.LosAngles', 2)", dbName, dbName)); err != nil {
		t.Fatal(err)
	}

	vg1, code := TaosGetTableVgID(conn, dbName, "d0")
	if code != 0 {
		t.Fatal("fail")
	}
	vg2, code := TaosGetTableVgID(conn, dbName, "d0")
	if code != 0 {
		t.Fatal("fail")
	}
	if vg1 != vg2 {
		t.Fatal("fail")
	}
	_, code = TaosGetTableVgID(conn, dbName, "d1")
	if code != 0 {
		t.Fatal("fail")
	}
	_, code = TaosGetTableVgID(conn, dbName, "d2")
	if code != 0 {
		t.Fatal("fail")
	}
}

// @author: xftan
// @date: 2023/10/13 11:32
// @description: test taos_get_tables_vgId
func TestTaosGetTablesVgID(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer TaosClose(conn)
	dbName := "tables_vg_id_test"

	_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	defer func() {
		_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	}()
	if err = exec(conn, fmt.Sprintf("create database %s", dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create stable %s.meters (ts timestamp, current float, voltage int, phase float) "+
		"tags (location binary(64), groupId int)", dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create table %s.d0 using %s.meters tags ('California.SanFrancisco', 1)", dbName, dbName)); err != nil {
		t.Fatal(err)
	}
	if err = exec(conn, fmt.Sprintf("create table %s.d1 using %s.meters tags ('California.LosAngles', 2)", dbName, dbName)); err != nil {
		t.Fatal(err)
	}
	var vgs1 []int
	var vgs2 []int
	var code int
	now := time.Now()
	vgs1, code = TaosGetTablesVgID(conn, dbName, []string{"d0", "d1"})
	t.Log(time.Since(now))
	if code != 0 {
		t.Fatal("fail")
	}
	assert.Equal(t, 2, len(vgs1))
	vgs2, code = TaosGetTablesVgID(conn, dbName, []string{"d0", "d1"})
	if code != 0 {
		t.Fatal("fail")
	}
	assert.Equal(t, 2, len(vgs2))
	assert.Equal(t, vgs2, vgs1)
}

func TestTaosSetConnMode(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	code := TaosSetConnMode(conn, 0, 1)
	if code != 0 {
		t.Errorf("TaosSetConnMode() error code= %d, msg: %s", code, TaosErrorStr(nil))
	}
}

func TestTaosGetCurrentDB(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	dbName := "current_db_test"
	_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	err = exec(conn, fmt.Sprintf("create database %s", dbName))
	assert.NoError(t, err)
	defer func() {
		_ = exec(conn, fmt.Sprintf("drop database if exists %s", dbName))
	}()
	_ = exec(conn, fmt.Sprintf("use %s", dbName))
	db, err := TaosGetCurrentDB(conn)
	assert.NoError(t, err)
	assert.Equal(t, dbName, db)
}

func TestTaosGetServerInfo(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	info := TaosGetServerInfo(conn)
	assert.NotEmpty(t, info)
}

func TestTaosOptionsConnection(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	ip := "192.168.9.9"
	app := "test_options_connection"
	// set ip
	code := TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, &ip)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	// set app
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &app)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	var values [][]driver.Value
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9' and user_app = 'test_options_connection'")
		assert.NoError(t, err)
		if len(values) == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 1, len(values))
	connID := values[0][0].(uint32)

	// clean app
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, nil)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9' and user_app = 'test_options_connection'")
		assert.NoError(t, err)
		if len(values) == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 0, len(values))
	values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9'")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, connID, values[0][0].(uint32))

	// set app
	app = "test_options_2"
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &app)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	for i := 0; i < 20; i++ {
		values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9' and user_app = 'test_options_2'")
		assert.NoError(t, err)
		if len(values) == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 1, len(values))
	assert.Equal(t, connID, values[0][0].(uint32))

	// clear ip
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, nil)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9' and user_app = 'test_options_2'")
		assert.NoError(t, err)
		if len(values) == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 0, len(values))
	values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_app = 'test_options_2'")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, connID, values[0][0].(uint32))

	// clean all
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_CLEAR, nil)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	for i := 0; i < 10; i++ {
		values, err = query(conn, fmt.Sprintf("select user_app,user_ip from performance_schema.perf_connections where conn_id = %d", connID))
		assert.NoError(t, err)
		if len(values) == 1 && values[0][0].(string) == "" && values[0][1].(string) == "" {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 1, len(values))
	assert.Equal(t, "", values[0][0].(string))
	// todo: engine should return empty string when connection ip is cleared
	//assert.Equal(t, "", values[0][1].(string))
	assert.NotEqual(t, ip, values[0][1].(string))
	ip = "192.168.9.9"
	app = "test_options_connection"
	// set ip
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_IP, &ip)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	// set app
	code = TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &app)
	assert.Equal(t, 0, code, TaosErrorStr(nil))
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select conn_id from performance_schema.perf_connections where user_ip = '192.168.9.9' and user_app = 'test_options_connection'")
		assert.NoError(t, err)
		if len(values) == 1 {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 1, len(values))
	assert.Equal(t, connID, values[0][0].(uint32))
}
