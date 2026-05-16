package unified

import (
	"database/sql/driver"
	"io"
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
)

// TestResultSetColumnMetadataMethods verifies the expected behavior for this scenario.
func TestResultSetColumnMetadataMethods(t *testing.T) {
	rs := &ResultSet{
		fieldsNames:     []string{"dec", "num", "unknown"},
		fieldsTypes:     []uint8{common.TSDB_DATA_TYPE_DECIMAL64, common.TSDB_DATA_TYPE_INT, 255},
		fieldsLengths:   []int64{8, 4, 0},
		fieldsPrecision: []int64{10, 0, 0},
		fieldsScale:     []int64{2, 0, 0},
	}

	precision, scale, ok := rs.ColumnTypePrecisionScale(0)
	if !ok || precision != 10 || scale != 2 {
		t.Fatalf("unexpected decimal precision/scale: (%d, %d, %t)", precision, scale, ok)
	}

	_, _, ok = rs.ColumnTypePrecisionScale(1)
	if ok {
		t.Fatal("expected non-decimal type to report ok=false")
	}

	cols := rs.Columns()
	if len(cols) != 3 || cols[0] != "dec" || cols[1] != "num" || cols[2] != "unknown" {
		t.Fatalf("unexpected columns: %v", cols)
	}

	if got := rs.ColumnTypeDatabaseTypeName(0); got != "DECIMAL" {
		t.Fatalf("unexpected database type name: %s", got)
	}

	length, lengthOK := rs.ColumnTypeLength(1)
	if length != 4 || !lengthOK {
		t.Fatalf("unexpected column length result: (%d, %t)", length, lengthOK)
	}

	if got := rs.ColumnTypeScanType(1); got != common.ColumnTypeMap[int(common.TSDB_DATA_TYPE_INT)] {
		t.Fatalf("unexpected scan type: %v", got)
	}
	if got := rs.ColumnTypeScanType(2); got != common.UnknownType {
		t.Fatalf("unexpected unknown scan type: %v", got)
	}
}

// TestResultSetColumnMetadataOutOfRange verifies the expected behavior for this scenario.
func TestResultSetColumnMetadataOutOfRange(t *testing.T) {
	rs := &ResultSet{
		fieldsNames:     []string{"dec"},
		fieldsTypes:     []uint8{common.TSDB_DATA_TYPE_DECIMAL64},
		fieldsLengths:   []int64{8},
		fieldsPrecision: []int64{10},
		fieldsScale:     []int64{2},
	}

	precision, scale, ok := rs.ColumnTypePrecisionScale(3)
	if ok || precision != 0 || scale != 0 {
		t.Fatalf("unexpected precision/scale result: (%d, %d, %t)", precision, scale, ok)
	}
	if got := rs.ColumnTypeDatabaseTypeName(3); got != "" {
		t.Fatalf("unexpected type name for out-of-range index: %q", got)
	}
	length, lengthOK := rs.ColumnTypeLength(3)
	if length != 0 || lengthOK {
		t.Fatalf("unexpected length result: (%d, %t)", length, lengthOK)
	}
	if got := rs.ColumnTypeScanType(3); got != common.UnknownType {
		t.Fatalf("unexpected scan type for out-of-range index: %v", got)
	}
}

// TestResultSetNextEOF verifies the expected behavior for this scenario.
func TestResultSetNextEOF(t *testing.T) {
	block := []byte{1}
	rs := &ResultSet{
		block:     block,
		blockPtr:  unsafe.Pointer(&block[0]),
		blockSize: 0,
	}

	err := rs.Next(make([]driver.Value, 0))
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if rs.block != nil || rs.blockPtr != nil {
		t.Fatalf("expected block cache released on EOF, got block=%v ptr=%v", rs.block, rs.blockPtr)
	}
}

// TestResultSetFormatTime verifies the expected behavior for this scenario.
func TestResultSetFormatTime(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	rs := &ResultSet{timezone: loc}
	v := rs.formatTime(0, common.PrecisionMilliSecond)
	got, ok := v.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", v)
	}
	if got.Location() != loc {
		t.Fatalf("unexpected location: %v", got.Location())
	}
}

// TestResultSetFreeResultWaitsForPrefetch verifies freeResult waits for in-flight prefetch completion.
func TestResultSetFreeResultWaitsForPrefetch(t *testing.T) {
	prefetchCh := make(chan fetchRawBlockResult)
	rs := &ResultSet{
		prefetching: true,
		prefetchCh:  prefetchCh,
	}

	done := make(chan error, 1)
	go func() {
		done <- rs.freeResult(1)
	}()

	select {
	case err := <-done:
		t.Fatalf("freeResult returned before prefetch completion: %v", err)
	default:
	}

	prefetchCh <- fetchRawBlockResult{completed: true}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error after completed prefetch drain, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("freeResult did not return after prefetch completion")
	}
}

// TestResultSetFreeResultSkipsRPCWhenCompleted verifies freeResult returns early once result is completed.
func TestResultSetFreeResultSkipsRPCWhenCompleted(t *testing.T) {
	rs := &ResultSet{completed: true}
	if err := rs.freeResult(1); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
