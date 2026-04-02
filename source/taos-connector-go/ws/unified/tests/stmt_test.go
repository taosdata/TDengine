package tests

import (
	"database/sql/driver"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

func TestUnifiedIntegrationStmt_AllTypesThreeRows(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_stmt_%d", time.Now().UnixNano())
	stableName := "st_all_types"
	tableName := "all_types"

	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
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
	_, err = client.Exec(0, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)

	ts1 := time.Unix(1722222222, 456000000).UTC().Round(time.Millisecond)
	ts2 := ts1.Add(time.Second)
	ts3 := ts1.Add(2 * time.Second)
	geo := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}

	insertSQL := fmt.Sprintf(
		"insert into ? using %s tags(?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		stableName,
	)
	insertStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = insertStmt.Close(0) })
	require.NoError(t, insertStmt.Prepare(0, insertSQL))
	require.NoError(t, insertStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: tableName,
			Tags:      []driver.Value{"tag_stmt"},
			Cols: [][]driver.Value{
				{ts1, ts2, ts3},
				{true, nil, false},
				{int8(-11), nil, int8(11)},
				{int16(-12), nil, int16(12)},
				{int32(-13), nil, int32(13)},
				{int64(-14), nil, int64(14)},
				{uint8(25), nil, uint8(35)},
				{uint16(26), nil, uint16(36)},
				{uint32(27), nil, uint32(37)},
				{uint64(28), nil, uint64(38)},
				{float32(11.5), nil, float32(13.5)},
				{float64(12.5), nil, float64(14.5)},
				{[]byte("bin_s1"), nil, []byte("bin_s3")},
				{"nchar_s1", nil, "nchar_s3"},
				{[]byte{0x11, 0x12}, nil, []byte{0x13, 0x14}},
				{geo, nil, geo},
				{"21.4300", nil, "87.6500"},
			},
		},
	}))
	affected, err := insertStmt.Exec(0)
	require.NoError(t, err)
	require.Equal(t, 3, affected)

	querySQL := fmt.Sprintf(
		"select tg,ts,c_bool,c_tinyint,c_smallint,c_int,c_bigint,c_utinyint,c_usmallint,c_uint,c_ubigint,c_float,c_double,c_binary,c_nchar,c_varbinary,c_geometry,c_decimal "+
			"from %s where tbname = ? and ts >= ? order by ts",
		stableName,
	)
	queryStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queryStmt.Close(0) })
	require.NoError(t, queryStmt.Prepare(0, querySQL))
	require.NoError(t, queryStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{tableName},
				{ts1},
			},
		},
	}))
	_, err = queryStmt.Exec(0)
	require.NoError(t, err)

	rows, err := queryStmt.UseResult(0)
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	allRows := readAllResultRows(t, rows, 18)
	require.Len(t, allRows, 3)

	row1 := allRows[0]
	requireValueEqual(t, row1[0], "tag_stmt")
	requireTimeEqual(t, row1[1], ts1)
	requireValueEqual(t, row1[2], true)
	requireValueEqual(t, row1[3], int8(-11))
	requireValueEqual(t, row1[4], int16(-12))
	requireValueEqual(t, row1[5], int32(-13))
	requireValueEqual(t, row1[6], int64(-14))
	requireValueEqual(t, row1[7], uint8(25))
	requireValueEqual(t, row1[8], uint16(26))
	requireValueEqual(t, row1[9], uint32(27))
	requireValueEqual(t, row1[10], uint64(28))
	requireValueEqual(t, row1[11], float32(11.5))
	requireValueEqual(t, row1[12], float64(12.5))
	requireValueEqual(t, row1[13], "bin_s1")
	requireValueEqual(t, row1[14], "nchar_s1")
	requireValueEqual(t, row1[15], []byte{0x11, 0x12})
	requireValueEqual(t, row1[16], geo)
	requireValueEqual(t, row1[17], "21.4300")

	row2 := allRows[1]
	requireValueEqual(t, row2[0], "tag_stmt")
	requireTimeEqual(t, row2[1], ts2)
	for i := 2; i < len(row2); i++ {
		require.Nil(t, row2[i], "row2 col[%d] should be nil", i)
	}

	row3 := allRows[2]
	requireValueEqual(t, row3[0], "tag_stmt")
	requireTimeEqual(t, row3[1], ts3)
	requireValueEqual(t, row3[2], false)
	requireValueEqual(t, row3[3], int8(11))
	requireValueEqual(t, row3[4], int16(12))
	requireValueEqual(t, row3[5], int32(13))
	requireValueEqual(t, row3[6], int64(14))
	requireValueEqual(t, row3[7], uint8(35))
	requireValueEqual(t, row3[8], uint16(36))
	requireValueEqual(t, row3[9], uint32(37))
	requireValueEqual(t, row3[10], uint64(38))
	requireValueEqual(t, row3[11], float32(13.5))
	requireValueEqual(t, row3[12], float64(14.5))
	requireValueEqual(t, row3[13], "bin_s3")
	requireValueEqual(t, row3[14], "nchar_s3")
	requireValueEqual(t, row3[15], []byte{0x13, 0x14})
	requireValueEqual(t, row3[16], geo)
	requireValueEqual(t, row3[17], "87.6500")
}

func TestUnifiedIntegrationStmt_MultiBindCrossTableTagFirstWins(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_stmt_bind_merge_%d", time.Now().UnixNano())
	stableName := "st_bind_merge"

	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
	})

	_, err = client.Exec(0, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)
	_, err = client.Exec(0, fmt.Sprintf("create table if not exists %s(ts timestamp, c1 int) tags(tg int)", stableName))
	require.NoError(t, err)

	insertSQL := fmt.Sprintf("insert into ? using %s tags(?) values(?,?)", stableName)
	insertStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = insertStmt.Close(0) })
	require.NoError(t, insertStmt.Prepare(0, insertSQL))

	ts1 := time.Unix(1723333333, 111000000).UTC().Round(time.Millisecond)
	ts2 := ts1.Add(time.Second)
	ts3 := ts1.Add(2 * time.Second)

	require.NoError(t, insertStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb1",
			Tags:      []driver.Value{int32(1)},
			Cols: [][]driver.Value{
				{ts1},
				{nil},
			},
		},
	}))
	require.NoError(t, insertStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb2",
			Tags:      []driver.Value{int32(2)},
			Cols: [][]driver.Value{
				{ts2},
				{int32(11)},
			},
		},
	}))
	require.NoError(t, insertStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb1",
			Tags:      []driver.Value{int32(2)},
			Cols: [][]driver.Value{
				{ts3},
				{int32(22)},
			},
		},
	}))

	affected, err := insertStmt.Exec(0)
	require.NoError(t, err)
	require.Equal(t, 3, affected)

	rows, err := client.Query(0, fmt.Sprintf("select tbname,tg,ts,c1 from %s order by tbname,ts", stableName))
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	allRows := readAllResultRows(t, rows, 4)
	require.Len(t, allRows, 3)

	// tb1 keeps the first tag (1), while c1 values come from all binds in order.
	row1 := allRows[0]
	requireValueEqual(t, row1[0], "tb1")
	requireValueEqual(t, row1[1], int32(1))
	requireTimeEqual(t, row1[2], ts1)
	require.Nil(t, row1[3])

	row2 := allRows[1]
	requireValueEqual(t, row2[0], "tb1")
	requireValueEqual(t, row2[1], int32(1))
	requireTimeEqual(t, row2[2], ts3)
	requireValueEqual(t, row2[3], int32(22))

	row3 := allRows[2]
	requireValueEqual(t, row3[0], "tb2")
	requireValueEqual(t, row3[1], int32(2))
	requireTimeEqual(t, row3[2], ts2)
	requireValueEqual(t, row3[3], int32(11))
}
