package sqlparser

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func isTypedNilSQLNode(node SQLNode) bool {
	if node == nil {
		return false
	}
	v := reflect.ValueOf(node)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func assertWalkNoTypedNil(t *testing.T, root SQLNode, label string) {
	t.Helper()
	visited := 0
	if err := Walk(func(node SQLNode) (bool, error) {
		visited++
		if isTypedNilSQLNode(node) {
			return false, fmt.Errorf("%s: typed-nil node visited: %T", label, node)
		}
		return true, nil
	}, root); err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if visited == 0 {
		t.Fatalf("walk visited no nodes for %T", root)
	}
}

func collectStatementParseFormatWalkSQL(t *testing.T) []string {
	t.Helper()
	seen := map[string]struct{}{}
	out := make([]string, 0, 1024)
	add := func(sql string) {
		if sql == "" {
			return
		}
		if _, ok := seen[sql]; ok {
			return
		}
		seen[sql] = struct{}{}
		out = append(out, sql)
	}

	for _, sql := range gatherRoundTripSQLFromCommandTests(t) {
		add(sql)
	}
	for _, sql := range gatherRoundTripSQLFromValidCorpus(t) {
		add(sql)
	}
	for _, sql := range gatherRoundTripSQLFromPassReport(t) {
		add(sql)
	}
	for _, tc := range loadStatementBranchCases(t) {
		add(tc.sql)
	}
	for _, tc := range loadSelectBranchCases(t) {
		add(tc.sql)
	}
	for _, tc := range loadSelectNestedCases(t) {
		add(tc.sql)
	}

	// Keep write/insert corpus lightweight by adding per-type representatives + samples.
	writeRep := map[string]string{}
	for _, sql := range loadWriteSQLCases(t) {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse write corpus failed while collecting representatives: %v sql=%q", err, sql)
		}
		typ := normalizeStmtTypeName(fmt.Sprintf("%T", stmt))
		if _, ok := writeRep[typ]; !ok {
			writeRep[typ] = sql
		}
	}
	writeTypes := make([]string, 0, len(writeRep))
	for typ := range writeRep {
		writeTypes = append(writeTypes, typ)
	}
	slices.Sort(writeTypes)
	for _, typ := range writeTypes {
		add(writeRep[typ])
	}

	insertSQLs := loadInsertSQLCases(t)
	for i := 0; i < len(insertSQLs); i += 30 {
		add(insertSQLs[i])
	}
	if len(insertSQLs) > 0 {
		add(insertSQLs[len(insertSQLs)-1])
	}

	return out
}

func TestWalk_SkipsTypedNilSQLNode(t *testing.T) {
	var sel *SelectStmt
	visited := 0
	if err := Walk(func(node SQLNode) (bool, error) {
		visited++
		return true, nil
	}, sel); err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if visited != 0 {
		t.Fatalf("typed nil node should be skipped, visited=%d", visited)
	}
}

func TestStatementParseFormatWalk_NoTypedNilNodes(t *testing.T) {
	sqls := collectStatementParseFormatWalkSQL(t)
	if len(sqls) < 500 {
		t.Fatalf("statement parse/format/walk coverage set too small: got=%d", len(sqls))
	}

	for i, sql := range sqls {
		t.Run(fmt.Sprintf("stmt_%04d", i+1), func(t *testing.T) {
			stmt1, err := Parse(sql)
			if err != nil {
				t.Fatalf("parse original failed: %v sql=%q", err, sql)
			}
			assertWalkNoTypedNil(t, stmt1, "parse")

			s1 := formatStatementForRoundTrip(t, stmt1)
			stmt2, err := Parse(s1)
			if err != nil {
				t.Fatalf("parse formatted failed: %v original=%q formatted=%q", err, sql, s1)
			}
			assertWalkNoTypedNil(t, stmt2, "parse(format)")
		})
	}
}
