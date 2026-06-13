package sqlparser

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func formatStatementForRoundTrip(t *testing.T, stmt Statement) string {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("format panic for %T: %v", stmt, r)
		}
	}()
	tb := newTB()
	stmt.Format(tb)
	out := strings.TrimSpace(tb.String())
	if out == "" {
		t.Fatalf("empty format output for %T", stmt)
	}
	return out
}

func semanticEqualValue(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() {
		return !a.IsValid() && !b.IsValid()
	}
	if a.Type() != b.Type() {
		return false
	}

	switch a.Kind() {
	case reflect.Interface, reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() && b.IsNil()
		}
		return semanticEqualValue(a.Elem(), b.Elem())
	case reflect.Slice:
		// Treat nil and empty slices as equal.
		if a.Len() == 0 && b.Len() == 0 {
			return true
		}
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !semanticEqualValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Array:
		for i := 0; i < a.Len(); i++ {
			if !semanticEqualValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if !semanticEqualValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		// Treat nil and empty maps as equal.
		if a.Len() == 0 && b.Len() == 0 {
			return true
		}
		if a.Len() != b.Len() {
			return false
		}
		iter := a.MapRange()
		for iter.Next() {
			av := iter.Value()
			bv := b.MapIndex(iter.Key())
			if !bv.IsValid() {
				return false
			}
			if !semanticEqualValue(av, bv) {
				return false
			}
		}
		return true
	case reflect.Bool:
		return a.Bool() == b.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return a.Int() == b.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return a.Uint() == b.Uint()
	case reflect.Float32, reflect.Float64:
		return a.Float() == b.Float()
	case reflect.Complex64, reflect.Complex128:
		return a.Complex() == b.Complex()
	case reflect.String:
		return a.String() == b.String()
	case reflect.Chan, reflect.Func:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() && b.IsNil()
		}
		return a.Pointer() == b.Pointer()
	default:
		if !a.CanInterface() || !b.CanInterface() {
			return a.Kind() == b.Kind()
		}
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}
}

func statementsSemanticallyEqual(a, b Statement) bool {
	return semanticEqualValue(reflect.ValueOf(a), reflect.ValueOf(b))
}

func runStatementRoundTrip(t *testing.T, sql string) {
	t.Helper()
	stmt1, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse original failed: %v sql=%q", err, sql)
	}
	s1 := formatStatementForRoundTrip(t, stmt1)

	stmt2, err := Parse(s1)
	if err != nil {
		t.Fatalf("parse formatted failed: %v original=%q formatted=%q stmt1=%T", err, sql, s1, stmt1)
	}
	s2 := formatStatementForRoundTrip(t, stmt2)

	if !statementsSemanticallyEqual(stmt1, stmt2) {
		t.Fatalf("statement mismatch after round-trip\nsql=%q\nformatted1=%q\nformatted2=%q\ntype1=%T\ntype2=%T\nstmt1=%#v\nstmt2=%#v",
			sql, s1, s2, stmt1, stmt2, stmt1, stmt2)
	}
	if s1 != s2 {
		t.Fatalf("format is not idempotent\nsql=%q\nformatted1=%q\nformatted2=%q\ntype=%T",
			sql, s1, s2, stmt1)
	}
}

func gatherRoundTripSQLFromCommandTests(t *testing.T) []string {
	t.Helper()
	files := []string{
		"command_entry_statement_test.go",
		"command_matrix_test.go",
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, 512)
	for _, f := range files {
		for _, sql := range extractSQLLiterals(t, f) {
			if _, ok := seen[sql]; ok {
				continue
			}
			seen[sql] = struct{}{}
			out = append(out, sql)
		}
	}
	return out
}

func gatherRoundTripSQLFromValidCorpus(t *testing.T) []string {
	t.Helper()
	cases := loadValidSQLCases(t)
	out := make([]string, 0, len(cases))
	for _, tc := range cases {
		if strings.Contains(tc.keyAssert, "roundtrip=false") {
			continue
		}
		out = append(out, tc.sql)
	}
	return out
}

func gatherRoundTripSQLFromPassReport(t *testing.T) []string {
	t.Helper()
	paths := []string{
		"reports/test_sql_pass.md",
		"reports/archive/2026-02-13/test_sql_pass.md",
	}
	var data []byte
	var err error
	found := false
	for _, p := range paths {
		data, err = os.ReadFile(p)
		if err == nil {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	out := make([]string, 0, 256)
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "- `") || !strings.HasSuffix(line, "`") {
			continue
		}
		sql := strings.TrimPrefix(line, "- `")
		sql = strings.TrimSuffix(sql, "`")
		sql = strings.TrimSpace(sql)
		sql = strings.ReplaceAll(sql, "\\`", "`")
		if sql == "" {
			continue
		}
		out = append(out, sql)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan pass sql report failed: %v", err)
	}
	return out
}

func TestStatementRoundTrip_CommandSuites(t *testing.T) {
	sqls := gatherRoundTripSQLFromCommandTests(t)
	if len(sqls) == 0 {
		t.Fatalf("no command sql extracted")
	}
	for i, sql := range sqls {
		t.Run(fmt.Sprintf("cmd_%03d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}

func TestStatementRoundTrip_ValidCorpus(t *testing.T) {
	sqls := gatherRoundTripSQLFromValidCorpus(t)
	if len(sqls) == 0 {
		t.Fatalf("no valid corpus sql loaded")
	}
	for i, sql := range sqls {
		t.Run(fmt.Sprintf("corpus_%03d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}

func TestStatementRoundTrip_PassReport(t *testing.T) {
	sqls := gatherRoundTripSQLFromPassReport(t)
	if len(sqls) == 0 {
		t.Skip("no pass-report sql loaded")
	}
	for i, sql := range sqls {
		t.Run(fmt.Sprintf("report_%03d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}
