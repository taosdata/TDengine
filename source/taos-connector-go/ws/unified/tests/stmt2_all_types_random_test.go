package tests

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

const (
	stmt2AllTypesRowCount = 10000
	stmt2SeedEnvName      = "TD_UNIFIED_STMT2_SEED"
)

type stmt2AllTypesCase struct {
	name string
	ddl  string
	gen  func(r *rand.Rand, rowIdx int) driver.Value
}

type stmt2ExpectedRow struct {
	ts time.Time
	v  driver.Value
}

func TestUnifiedIntegrationStmt2_AllTypesRandom10000Rows(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	rootSeed := stmt2TestSeed(t)
	reproCmd := fmt.Sprintf("%s=%d go test ./ws/unified/tests -run TestUnifiedIntegrationStmt2_AllTypesRandom10000Rows -count=1", stmt2SeedEnvName, rootSeed)
	t.Logf("stmt2 all-types random test, rowCount=%d, seed=%d", stmt2AllTypesRowCount, rootSeed)
	t.Logf("repro command: %s", reproCmd)

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_stmt2_all_types_%d", time.Now().UnixNano())
	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
		assert.NoError(t, err)
	})

	_, err = client.Exec(0, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)

	cases := stmt2AllTypesCases()
	for idx := range cases {
		tc := cases[idx]
		caseSeed := rootSeed + int64((idx+1)*1000003)
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("type=%s caseSeed=%d", tc.name, caseSeed)

			tableName := fmt.Sprintf("stmt2_all_%02d_%s", idx, tc.name)
			_, err = client.Exec(0, fmt.Sprintf("create table if not exists %s(ts timestamp, c1 %s)", tableName, tc.ddl))
			require.NoError(t, err)

			expectedRows, nilCount := stmt2BuildRandomRows(stmt2AllTypesRowCount, caseSeed, tc.gen)
			require.Greater(t, nilCount, 0, "type=%s must include nil rows, seed=%d", tc.name, caseSeed)
			require.Less(t, nilCount, stmt2AllTypesRowCount, "type=%s must include non-nil rows, seed=%d", tc.name, caseSeed)

			insertSQL := fmt.Sprintf("insert into %s values(?,?)", tableName)
			err = stmt2InsertRows(t, client, insertSQL, expectedRows)
			require.NoError(t, err, "stmt2 insert failed, type=%s caseSeed=%d rootSeed=%d repro=%s", tc.name, caseSeed, rootSeed, reproCmd)

			require.Equal(t, stmt2AllTypesRowCount, queryCount(t, client, fmt.Sprintf("select count(*) from %s", tableName)), "row count mismatch after insert, type=%s caseSeed=%d", tc.name, caseSeed)

			sqlGotRows := stmt2QueryRowsBySQL(t, client, tableName)
			stmt2RequireRowsMatchExpected(t, sqlGotRows, expectedRows, "sql", tc.name, rootSeed, caseSeed)

			stmt2GotRows := stmt2QueryRowsByStmt2(t, client, tableName, expectedRows[0].ts)
			stmt2RequireRowsMatchExpected(t, stmt2GotRows, expectedRows, "stmt2", tc.name, rootSeed, caseSeed)
		})
	}
}

func stmt2AllTypesCases() []stmt2AllTypesCase {
	return []stmt2AllTypesCase{
		{
			name: "bool",
			ddl:  "bool",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return r.Intn(2) == 0
			},
		},
		{
			name: "tinyint",
			ddl:  "tinyint",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return int8(r.Intn(256) - 128)
			},
		},
		{
			name: "smallint",
			ddl:  "smallint",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return int16(r.Intn(65536) - 32768)
			},
		},
		{
			name: "int",
			ddl:  "int",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return int32(r.Int31n(2000000000) - 1000000000)
			},
		},
		{
			name: "bigint",
			ddl:  "bigint",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return int64(r.Int63n(2000000000000) - 1000000000000)
			},
		},
		{
			name: "utinyint",
			ddl:  "tinyint unsigned",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return uint8(r.Intn(256))
			},
		},
		{
			name: "usmallint",
			ddl:  "smallint unsigned",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return uint16(r.Intn(65536))
			},
		},
		{
			name: "uint",
			ddl:  "int unsigned",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return uint32(r.Uint32())
			},
		},
		{
			name: "ubigint",
			ddl:  "bigint unsigned",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return (uint64(r.Uint32()) << 32) | uint64(r.Uint32())
			},
		},
		{
			name: "float",
			ddl:  "float",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return float32(r.Intn(200000)-100000) / float32(100)
			},
		},
		{
			name: "double",
			ddl:  "double",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return float64(r.Int63n(200000000000)-100000000000) / float64(10000)
			},
		},
		{
			name: "binary",
			ddl:  "binary(64)",
			gen: func(r *rand.Rand, rowIdx int) driver.Value {
				return fmt.Sprintf("bin_%05d_%s", rowIdx, stmt2RandomASCII(r, 4, 12))
			},
		},
		{
			name: "nchar",
			ddl:  "nchar(64)",
			gen: func(r *rand.Rand, rowIdx int) driver.Value {
				return fmt.Sprintf("nchar_%05d_%s", rowIdx, stmt2RandomASCII(r, 4, 12))
			},
		},
		{
			name: "varbinary",
			ddl:  "varbinary(64)",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return stmt2RandomBytes(r, 3, 16)
			},
		},
		{
			name: "geometry",
			ddl:  "geometry(100)",
			gen: func(r *rand.Rand, _ int) driver.Value {
				return stmt2RandomPointWKB(r)
			},
		},
		{
			name: "decimal",
			ddl:  "decimal(20,4)",
			gen: func(r *rand.Rand, _ int) driver.Value {
				sign := ""
				if r.Intn(2) == 0 {
					sign = "-"
				}
				intPart := r.Int63n(1000000000)
				frac := r.Intn(10000)
				return fmt.Sprintf("%s%d.%04d", sign, intPart, frac)
			},
		},
		{
			name: "timestamp",
			ddl:  "timestamp",
			gen: func(r *rand.Rand, _ int) driver.Value {
				base := time.Unix(1700000000, 0).UTC()
				offsetMillis := r.Int63n(90 * 24 * 60 * 60 * 1000)
				return base.Add(time.Duration(offsetMillis) * time.Millisecond).Round(time.Millisecond)
			},
		},
	}
}

func stmt2TestSeed(t *testing.T) int64 {
	t.Helper()
	raw, ok := os.LookupEnv(stmt2SeedEnvName)
	if !ok || raw == "" {
		return time.Now().UnixNano()
	}
	seed, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, "invalid %s=%q", stmt2SeedEnvName, raw)
	return seed
}

func stmt2BuildRandomRows(rowCount int, seed int64, gen func(r *rand.Rand, rowIdx int) driver.Value) ([]stmt2ExpectedRow, int) {
	r := rand.New(rand.NewSource(seed))
	baseTS := time.Unix(1730000000, 0).UTC().Round(time.Millisecond)
	rows := make([]stmt2ExpectedRow, rowCount)

	nilCount := 0
	nonNilCount := 0
	for i := 0; i < rowCount; i++ {
		ts := baseTS.Add(time.Duration(i) * time.Millisecond)

		var val driver.Value
		// Keep random and deterministic, and guarantee nil appears frequently.
		if i%97 == 0 || r.Intn(10) == 0 {
			val = nil
			nilCount++
		} else {
			val = gen(r, i)
			nonNilCount++
		}

		rows[i] = stmt2ExpectedRow{
			ts: ts,
			v:  val,
		}
	}

	if nilCount == 0 {
		rows[0].v = nil
		nilCount++
		nonNilCount--
	}
	if nonNilCount == 0 {
		rows[len(rows)-1].v = gen(r, len(rows)-1)
		nilCount--
	}
	return rows, nilCount
}

func stmt2InsertRows(t *testing.T, client *unified.Client, sql string, expectedRows []stmt2ExpectedRow) error {
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

	tsCol := make([]driver.Value, len(expectedRows))
	c1Col := make([]driver.Value, len(expectedRows))
	for i := 0; i < len(expectedRows); i++ {
		tsCol[i] = expectedRows[i].ts
		c1Col[i] = expectedRows[i].v
	}

	if err = stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				tsCol,
				c1Col,
			},
		},
	}); err != nil {
		return err
	}

	affected, err := stmt.Exec(0)
	if err != nil {
		return err
	}
	if affected != len(expectedRows) {
		return fmt.Errorf("affected rows mismatch: want=%d got=%d", len(expectedRows), affected)
	}
	return nil
}

func stmt2QueryRowsBySQL(t *testing.T, client *unified.Client, tableName string) [][]driver.Value {
	t.Helper()
	rows, err := client.Query(0, fmt.Sprintf("select ts,c1 from %s order by ts", tableName))
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })
	return readAllResultRows(t, rows, 2)
}

func stmt2QueryRowsByStmt2(t *testing.T, client *unified.Client, tableName string, startTS time.Time) [][]driver.Value {
	t.Helper()

	querySQL := fmt.Sprintf("select ts,c1 from %s where ts >= ? order by ts", tableName)
	queryStmt, err := client.InitStmt(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = queryStmt.Close(0) })

	require.NoError(t, queryStmt.Prepare(0, querySQL))
	require.NoError(t, queryStmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{startTS},
			},
		},
	}))

	_, err = queryStmt.Exec(0)
	require.NoError(t, err)

	rows, err := queryStmt.UseResult(0)
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	return readAllResultRows(t, rows, 2)
}

func stmt2RequireRowsMatchExpected(t *testing.T, gotRows [][]driver.Value, expectedRows []stmt2ExpectedRow, source string, typeName string, rootSeed int64, caseSeed int64) {
	t.Helper()

	reproCmd := fmt.Sprintf("%s=%d go test ./ws/unified/tests -run TestUnifiedIntegrationStmt2_AllTypesRandom10000Rows/%s -count=1", stmt2SeedEnvName, rootSeed, typeName)
	require.Len(t, gotRows, len(expectedRows), "%s row count mismatch type=%s rootSeed=%d caseSeed=%d repro=%s", source, typeName, rootSeed, caseSeed, reproCmd)

	for rowIdx := 0; rowIdx < len(expectedRows); rowIdx++ {
		got := gotRows[rowIdx]
		require.Len(t, got, 2, "%s col count mismatch type=%s row=%d rootSeed=%d caseSeed=%d", source, typeName, rowIdx, rootSeed, caseSeed)

		gotTS, ok := got[0].(time.Time)
		if !ok {
			t.Fatalf("%s ts type mismatch type=%s row=%d want=time.Time got=%T rootSeed=%d caseSeed=%d repro=%s", source, typeName, rowIdx, got[0], rootSeed, caseSeed, reproCmd)
		}
		require.Equal(t, expectedRows[rowIdx].ts.UnixNano(), gotTS.UnixNano(), "%s ts mismatch type=%s row=%d rootSeed=%d caseSeed=%d repro=%s", source, typeName, rowIdx, rootSeed, caseSeed, reproCmd)

		if err := stmt2CompareValue(got[1], expectedRows[rowIdx].v); err != nil {
			t.Fatalf(
				"%s value mismatch type=%s row=%d ts=%s want=%#v(%T) got=%#v(%T) err=%v rootSeed=%d caseSeed=%d repro=%s",
				source,
				typeName,
				rowIdx,
				expectedRows[rowIdx].ts.Format(time.RFC3339Nano),
				expectedRows[rowIdx].v,
				expectedRows[rowIdx].v,
				got[1],
				got[1],
				err,
				rootSeed,
				caseSeed,
				reproCmd,
			)
		}
	}
}

func stmt2CompareValue(got driver.Value, want driver.Value) error {
	if want == nil {
		if got != nil {
			return fmt.Errorf("want=nil got=%#v(%T)", got, got)
		}
		return nil
	}
	if got == nil {
		return fmt.Errorf("want=%#v(%T) got=nil", want, want)
	}

	switch w := want.(type) {
	case float32:
		g, ok := got.(float32)
		if !ok {
			return fmt.Errorf("float32 type mismatch got=%T", got)
		}
		if math.Abs(float64(g-w)) > 0.0001 {
			return fmt.Errorf("float32 delta too large want=%v got=%v", w, g)
		}
		return nil
	case float64:
		g, ok := got.(float64)
		if !ok {
			return fmt.Errorf("float64 type mismatch got=%T", got)
		}
		if math.Abs(g-w) > 0.0000001 {
			return fmt.Errorf("float64 delta too large want=%v got=%v", w, g)
		}
		return nil
	case time.Time:
		g, ok := got.(time.Time)
		if !ok {
			return fmt.Errorf("time.Time type mismatch got=%T", got)
		}
		if g.UnixNano() != w.UnixNano() {
			return fmt.Errorf("time mismatch want=%s got=%s", w.Format(time.RFC3339Nano), g.Format(time.RFC3339Nano))
		}
		return nil
	case []byte:
		g, ok := got.([]byte)
		if !ok {
			return fmt.Errorf("[]byte type mismatch got=%T", got)
		}
		if !bytes.Equal(g, w) {
			return fmt.Errorf("[]byte mismatch want=%x got=%x", w, g)
		}
		return nil
	default:
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("deep equal failed")
		}
		return nil
	}
}

func stmt2RandomASCII(r *rand.Rand, minLen int, maxLen int) string {
	if maxLen < minLen {
		maxLen = minLen
	}
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	n := minLen + r.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func stmt2RandomBytes(r *rand.Rand, minLen int, maxLen int) []byte {
	if maxLen < minLen {
		maxLen = minLen
	}
	n := minLen + r.Intn(maxLen-minLen+1)
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = byte(r.Intn(256))
	}
	return out
}

func stmt2RandomPointWKB(r *rand.Rand) []byte {
	x := r.Float64()*360 - 180
	y := r.Float64()*180 - 90
	out := make([]byte, 1+4+8+8)
	out[0] = 1 // little-endian
	binary.LittleEndian.PutUint32(out[1:5], 1)
	binary.LittleEndian.PutUint64(out[5:13], math.Float64bits(x))
	binary.LittleEndian.PutUint64(out[13:21], math.Float64bits(y))
	return out
}
