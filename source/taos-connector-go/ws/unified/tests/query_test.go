package tests

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

const unifiedDefaultIntegrationDSN = "root:taosdata@ws(127.0.0.1:6041)/"

func openUnifiedIntegrationClient(t *testing.T) *unified.Client {
	t.Helper()

	dsn := unifiedDefaultIntegrationDSN

	client, err := unified.Open(dsn)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: unified.Open failed: %v", err)
	}
	if _, err = client.Exec(0, "select server_version()"); err != nil {
		client.Close()
		t.Fatalf("integration test requires taosadapter/taosd: health check failed: %v", err)
	}
	return client
}

func readAllResultRows(t *testing.T, rows *unified.ResultSet, colCount int) [][]driver.Value {
	t.Helper()

	out := make([][]driver.Value, 0, 4)
	for {
		values := make([]driver.Value, colCount)
		err := rows.Next(values)
		if err == io.EOF {
			return out
		}
		require.NoError(t, err)
		out = append(out, append([]driver.Value(nil), values...))
	}
}

func requireTimeEqual(t *testing.T, got driver.Value, want time.Time) {
	t.Helper()

	gotTime, ok := got.(time.Time)
	require.True(t, ok, "expect time.Time got %T", got)
	require.Equal(t, want.UnixNano(), gotTime.UnixNano())
}

func requireValueEqual(t *testing.T, got driver.Value, want driver.Value) {
	t.Helper()
	require.True(t, reflect.DeepEqual(got, want), "value mismatch, want=%#v got=%#v", want, got)
}

func unixMilli(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func TestUnifiedIntegrationQuery_AllTypesThreeRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_query_%d", time.Now().UnixNano())
	stableName := "st_all_types"
	tableName := "all_types"

	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
		assert.NoError(t, err)
	})

	createSQL := fmt.Sprintf(
		"create table if not exists %s.%s("+
			"ts timestamp,"+
			"c_bool bool,"+
			"c_tinyint tinyint,"+
			"c_smallint smallint,"+
			"c_int int,"+
			"c_bigint bigint,"+
			"c_utinyint tinyint unsigned,"+
			"c_usmallint smallint unsigned,"+
			"c_uint int unsigned,"+
			"c_ubigint bigint unsigned,"+
			"c_float float,"+
			"c_double double,"+
			"c_binary binary(32),"+
			"c_nchar nchar(32),"+
			"c_varbinary varbinary(32),"+
			"c_geometry geometry(100),"+
			"c_decimal decimal(20,4)) "+
			"tags(tg nchar(32))",
		dbName, stableName,
	)
	_, err = client.Exec(0, createSQL)
	require.NoError(t, err)
	_, err = client.Exec(0, fmt.Sprintf("create table if not exists %s.%s using %s.%s tags('tag_query')", dbName, tableName, dbName, stableName))
	require.NoError(t, err)

	ts1 := time.Unix(1711111111, 123000000).UTC().Round(time.Millisecond)
	ts2 := ts1.Add(time.Second)
	ts3 := ts1.Add(2 * time.Second)
	geo := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}

	insertSQL1 := fmt.Sprintf(
		"insert into %s.%s values(%d,true,-1,-2,-3,-4,5,6,7,8,1.5,2.5,'bin_r1','nchar_r1','varb_r1','POINT(100 100)',12.3400)",
		dbName, tableName, unixMilli(ts1),
	)
	_, err = client.Exec(0, insertSQL1)
	require.NoError(t, err)

	insertSQL2 := fmt.Sprintf(
		"insert into %s.%s values(%d,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)",
		dbName, tableName, unixMilli(ts2),
	)
	_, err = client.Exec(0, insertSQL2)
	require.NoError(t, err)

	insertSQL3 := fmt.Sprintf(
		"insert into %s.%s values(%d,false,1,2,3,4,15,16,17,18,3.5,4.5,'bin_r3','nchar_r3','varb_r3','POINT(100 100)',98.7600)",
		dbName, tableName, unixMilli(ts3),
	)
	_, err = client.Exec(0, insertSQL3)
	require.NoError(t, err)

	querySQL := fmt.Sprintf(
		"select tg,ts,c_bool,c_tinyint,c_smallint,c_int,c_bigint,c_utinyint,c_usmallint,c_uint,c_ubigint,c_float,c_double,c_binary,c_nchar,c_varbinary,c_geometry,c_decimal "+
			"from %s.%s where tbname = '%s' order by ts",
		dbName, stableName, tableName,
	)
	rows, err := client.Query(0, querySQL)
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	allRows := readAllResultRows(t, rows, 18)
	require.Len(t, allRows, 3)

	row1 := allRows[0]
	requireValueEqual(t, row1[0], "tag_query")
	requireTimeEqual(t, row1[1], ts1)
	requireValueEqual(t, row1[2], true)
	requireValueEqual(t, row1[3], int8(-1))
	requireValueEqual(t, row1[4], int16(-2))
	requireValueEqual(t, row1[5], int32(-3))
	requireValueEqual(t, row1[6], int64(-4))
	requireValueEqual(t, row1[7], uint8(5))
	requireValueEqual(t, row1[8], uint16(6))
	requireValueEqual(t, row1[9], uint32(7))
	requireValueEqual(t, row1[10], uint64(8))
	requireValueEqual(t, row1[11], float32(1.5))
	requireValueEqual(t, row1[12], float64(2.5))
	requireValueEqual(t, row1[13], "bin_r1")
	requireValueEqual(t, row1[14], "nchar_r1")
	requireValueEqual(t, row1[15], []byte("varb_r1"))
	requireValueEqual(t, row1[16], geo)
	requireValueEqual(t, row1[17], "12.3400")

	row2 := allRows[1]
	requireValueEqual(t, row2[0], "tag_query")
	requireTimeEqual(t, row2[1], ts2)
	for i := 2; i < len(row2); i++ {
		require.Nil(t, row2[i], "row2 col[%d] should be nil", i)
	}

	row3 := allRows[2]
	requireValueEqual(t, row3[0], "tag_query")
	requireTimeEqual(t, row3[1], ts3)
	requireValueEqual(t, row3[2], false)
	requireValueEqual(t, row3[3], int8(1))
	requireValueEqual(t, row3[4], int16(2))
	requireValueEqual(t, row3[5], int32(3))
	requireValueEqual(t, row3[6], int64(4))
	requireValueEqual(t, row3[7], uint8(15))
	requireValueEqual(t, row3[8], uint16(16))
	requireValueEqual(t, row3[9], uint32(17))
	requireValueEqual(t, row3[10], uint64(18))
	requireValueEqual(t, row3[11], float32(3.5))
	requireValueEqual(t, row3[12], float64(4.5))
	requireValueEqual(t, row3[13], "bin_r3")
	requireValueEqual(t, row3[14], "nchar_r3")
	requireValueEqual(t, row3[15], []byte("varb_r3"))
	requireValueEqual(t, row3[16], geo)
	requireValueEqual(t, row3[17], "98.7600")
}
