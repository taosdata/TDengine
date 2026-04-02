package tests

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

// TestUnifiedIntegrationStmt_WrongTypeValidation is intentionally aligned with
// the dotnet StmtTestWrongType idea:
// 1. Build a "type matrix": one table per target type for c1.
// 2. Build an "input matrix": a fixed set of runtime Go values.
// 3. For every (target type, input value) pair:
//   - Prepare stmt2 insert.
//   - Bind [ts, c1].
//   - Exec and assert success/failure based on expected compatibility.
//
// 4. Verify the final row count equals the number of successful inserts.
// 5. Keep dotnet parity for JSON tag behavior:
//   - insert with non-nil JSON tag
//   - insert with nil JSON tag
//   - verify total rows = 2
//
// Execution graph:
//
//	open client
//	   |
//	   v
//	create/use db
//	   |
//	   +--> json tag parity check --> count(json_stb)=2
//	   |
//	   v
//	for each target type table
//	   |
//	   +--> for each input value
//	   |       |
//	   |       +--> stmt prepare + bind + exec
//	   |       +--> assert success/fail matches expectation
//	   |
//	   +--> assert count(table) == successCount
//	   |
//	   v
//	drop db (cleanup)
func TestUnifiedIntegrationStmt_WrongTypeValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_stmt_wrong_type_%d", time.Now().UnixNano())
	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
		assert.NoError(t, err)
	})
	_, err = client.Exec(0, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)

	// Keep the json tag behavior check aligned with dotnet StmtTestWrongType.
	_, err = client.Exec(0, "create table if not exists test_json_stb(ts timestamp, c1 int) tags(t json)")
	require.NoError(t, err)
	require.NoError(t, stmtInsertWithTableAndTag(t, client, "insert into ? using test_json_stb tags(?) values(?,?)", "test_json", `{"a":"b"}`, time.Now().UTC(), int32(1)))
	require.NoError(t, stmtInsertWithTableAndTag(t, client, "insert into ? using test_json_stb tags(?) values(?,?)", "test_json_null", nil, time.Now().UTC(), int32(1)))
	require.Equal(t, 2, queryCount(t, client, "select count(*) from test_json_stb"))

	type targetCase struct {
		name      string
		columnDDL string
		acceptOps map[string]bool
	}
	type opCase struct {
		name  string
		value driver.Value
	}

	ops := []opCase{
		{name: "null", value: nil},
		{name: "datetime", value: time.Now().UTC().Round(time.Millisecond)},
		{name: "datetimeoffset", value: time.Now().In(time.FixedZone("UTC+8", 8*3600)).Round(time.Millisecond)},
		{name: "bool", value: true},
		{name: "int8", value: int8(2)},
		{name: "int16", value: int16(2)},
		{name: "int32", value: int32(2)},
		{name: "int64", value: int64(2)},
		{name: "float32", value: float32(2)},
		{name: "float64", value: float64(2)},
		{name: "uint8", value: uint8(2)},
		{name: "uint16", value: uint16(2)},
		{name: "uint32", value: uint32(2)},
		{name: "uint64", value: uint64(2)},
		{name: "string", value: "abc"},
		{name: "bytes", value: []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
	}

	targets := []targetCase{
		{name: "timestamp", columnDDL: "timestamp", acceptOps: map[string]bool{"null": true, "datetime": true, "datetimeoffset": true, "int64": true}},
		{name: "bool", columnDDL: "bool", acceptOps: map[string]bool{"null": true, "bool": true}},
		{name: "tinyint", columnDDL: "tinyint", acceptOps: map[string]bool{"null": true, "int8": true}},
		{name: "smallint", columnDDL: "smallint", acceptOps: map[string]bool{"null": true, "int16": true}},
		{name: "int", columnDDL: "int", acceptOps: map[string]bool{"null": true, "int32": true}},
		{name: "bigint", columnDDL: "bigint", acceptOps: map[string]bool{"null": true, "int64": true}},
		{name: "utinyint", columnDDL: "tinyint unsigned", acceptOps: map[string]bool{"null": true, "uint8": true}},
		{name: "usmallint", columnDDL: "smallint unsigned", acceptOps: map[string]bool{"null": true, "uint16": true}},
		{name: "uint", columnDDL: "int unsigned", acceptOps: map[string]bool{"null": true, "uint32": true}},
		{name: "ubigint", columnDDL: "bigint unsigned", acceptOps: map[string]bool{"null": true, "uint64": true}},
		{name: "float", columnDDL: "float", acceptOps: map[string]bool{"null": true, "float32": true}},
		{name: "double", columnDDL: "double", acceptOps: map[string]bool{"null": true, "float64": true}},
		{name: "binary", columnDDL: "binary(100)", acceptOps: map[string]bool{"null": true, "string": true, "bytes": true}},
		{name: "nchar", columnDDL: "nchar(100)", acceptOps: map[string]bool{"null": true, "string": true, "bytes": true}},
		{name: "varbinary", columnDDL: "varbinary(100)", acceptOps: map[string]bool{"null": true, "string": true, "bytes": true}},
		{name: "geometry", columnDDL: "geometry(100)", acceptOps: map[string]bool{"null": true, "bytes": true}},
	}

	for i := 0; i < len(targets); i++ {
		tc := targets[i]
		t.Run(tc.name, func(t *testing.T) {
			tableName := "test_wrong_type_" + strconv.Itoa(i)
			_, err = client.Exec(0, fmt.Sprintf("create table if not exists %s(ts timestamp, c1 %s)", tableName, tc.columnDDL))
			require.NoError(t, err)

			successCount := 0
			for j := 0; j < len(ops); j++ {
				op := ops[j]
				err = stmtInsertOneRow(t, client, fmt.Sprintf("insert into %s values(?,?)", tableName), time.Now().UTC(), op.value)
				if tc.acceptOps[op.name] {
					require.NoError(t, err, "op=%s should succeed for %s", op.name, tc.name)
					successCount++
				} else {
					require.Error(t, err, "op=%s should fail for %s", op.name, tc.name)
				}
			}
			require.Equal(t, successCount, queryCount(t, client, fmt.Sprintf("select count(*) from %s", tableName)))
		})
	}
}

func stmtInsertWithTableAndTag(t *testing.T, client *unified.Client, sql string, table string, tag driver.Value, ts time.Time, v driver.Value) error {
	t.Helper()
	stmt, err := client.InitStmt(0)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close(0)
	}()
	if err = stmt.Prepare(0, sql); err != nil {
		return err
	}
	if err = stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: table,
			Tags:      []driver.Value{tag},
			Cols: [][]driver.Value{
				{ts},
				{v},
			},
		},
	}); err != nil {
		return err
	}
	_, err = stmt.Exec(0)
	return err
}

func stmtInsertOneRow(t *testing.T, client *unified.Client, sql string, ts time.Time, v driver.Value) error {
	t.Helper()
	stmt, err := client.InitStmt(0)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close(0)
	}()
	if err = stmt.Prepare(0, sql); err != nil {
		return err
	}
	if err = stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{ts},
				{v},
			},
		},
	}); err != nil {
		return err
	}
	_, err = stmt.Exec(0)
	return err
}

func queryCount(t *testing.T, client *unified.Client, sql string) int {
	t.Helper()
	rows, err := client.Query(0, sql)
	require.NoError(t, err)
	require.NotNil(t, rows)
	defer func() {
		_ = rows.Close()
	}()
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	switch v := values[0].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		t.Fatalf("unexpected count type %T value=%v", values[0], values[0])
		return 0
	}
}
