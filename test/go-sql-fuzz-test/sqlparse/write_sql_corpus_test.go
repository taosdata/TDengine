package sqlparser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func loadWriteSQLCases(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "write_sql_cases.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open write sql corpus failed: %v", err)
	}
	defer f.Close()

	out := make([]string, 0, 512)
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan write sql corpus failed: %v", err)
	}
	return out
}

func isReadOnlyStatementType(typ string) bool {
	switch typ {
	case "SelectStmt", "ShowStmt", "DescribeStmt", "ExplainStmt":
		return true
	default:
		return false
	}
}

func expectedWriteStatementTypes() []string {
	want := make([]string, 0, 64)
	for _, typ := range expectedStatementTypes() {
		norm := normalizeStmtTypeName(typ)
		if isReadOnlyStatementType(norm) {
			continue
		}
		want = append(want, norm)
	}
	slices.Sort(want)
	return slices.Compact(want)
}

func TestWriteSQLCorpus_ParseAll(t *testing.T) {
	sqls := loadWriteSQLCases(t)
	if len(sqls) < 500 {
		t.Fatalf("expected at least 500 write sql cases, got %d", len(sqls))
	}

	expected := expectedWriteStatementTypes()
	expectedSet := make(map[string]struct{}, len(expected))
	for _, typ := range expected {
		expectedSet[typ] = struct{}{}
	}

	hit := make(map[string]int, len(expected))
	for i, sql := range sqls {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse write sql failed at #%d: %v, sql=%q", i+1, err, sql)
		}
		gotType := normalizeStmtTypeName(fmt.Sprintf("%T", stmt))
		if _, ok := expectedSet[gotType]; !ok {
			t.Fatalf("non-write or unexpected statement type at #%d: %T, sql=%q", i+1, stmt, sql)
		}
		hit[gotType]++
	}

	missing := make([]string, 0, 8)
	for _, typ := range expected {
		if hit[typ] == 0 {
			missing = append(missing, typ)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("write sql corpus missing statement types: %v", missing)
	}
}

func TestWriteSQLCorpus_PerTypeRepresentatives(t *testing.T) {
	sqls := loadWriteSQLCases(t)
	if len(sqls) == 0 {
		t.Fatalf("no write sql loaded")
	}

	expected := expectedWriteStatementTypes()
	rep := make(map[string]string, len(expected))
	for i, sql := range sqls {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse write sql failed at #%d: %v, sql=%q", i+1, err, sql)
		}
		typ := normalizeStmtTypeName(fmt.Sprintf("%T", stmt))
		if _, ok := rep[typ]; !ok {
			rep[typ] = sql
		}
	}

	for _, typ := range expected {
		sql, ok := rep[typ]
		if !ok {
			t.Fatalf("missing representative sql for write statement type: %s", typ)
		}
		t.Run(typ, func(t *testing.T) {
			stmt, err := Parse(sql)
			if err != nil {
				t.Fatalf("parse representative sql failed: %v, sql=%q", err, sql)
			}
			got := normalizeStmtTypeName(fmt.Sprintf("%T", stmt))
			if got != typ {
				t.Fatalf("representative type mismatch: got=%s want=%s sql=%q", got, typ, sql)
			}
			_ = formatStatementForRoundTrip(t, stmt)
		})
	}
}
