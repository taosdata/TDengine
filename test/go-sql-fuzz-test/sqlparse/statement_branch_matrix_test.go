package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type statementBranchCase struct {
	id         string
	stmtFamily string
	rule       string
	branchSig  string
	sql        string
	expect     string
	keyAssert  string
}

func loadStatementBranchCases(t *testing.T) []statementBranchCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "statement_branch_matrix.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open statement branch matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []statementBranchCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 7 {
			t.Fatalf("invalid statement branch matrix line %d: %q", line, s)
		}
		out = append(out, statementBranchCase{
			id:         cols[0],
			stmtFamily: cols[1],
			rule:       cols[2],
			branchSig:  cols[3],
			sql:        cols[4],
			expect:     cols[5],
			keyAssert:  cols[6],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan statement branch matrix failed: %v", err)
	}
	return out
}

func statementTypeName(stmt Statement) string {
	v := reflect.TypeOf(stmt)
	if v == nil {
		return ""
	}
	if v.Kind() == reflect.Pointer {
		return v.Elem().Name()
	}
	return v.Name()
}

func TestStatementBranchMatrix_RoundTrip(t *testing.T) {
	cases := loadStatementBranchCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty statement branch matrix")
	}

	coveredTypes := map[string]struct{}{}
	for _, tc := range cases {
		if tc.expect != "ok" && tc.expect != "ok_no_roundtrip" {
			t.Fatalf("[%s] expect must be ok|ok_no_roundtrip in statement_branch_matrix.tsv, got=%q", tc.id, tc.expect)
		}
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("[%s] parse failed: %v sql=%q", tc.id, err, tc.sql)
		}
		gotType := statementTypeName(stmt)
		if tc.stmtFamily != "" && gotType != tc.stmtFamily {
			t.Fatalf("[%s] stmt type mismatch: got=%q want=%q sql=%q", tc.id, gotType, tc.stmtFamily, tc.sql)
		}
		if tc.keyAssert != "" {
			assertKeyFields(t, stmt, tc.keyAssert)
		}
		if tc.expect == "ok" {
			runStatementRoundTrip(t, tc.sql)
		}
		coveredTypes[reflect.TypeOf(stmt).String()] = struct{}{}
	}

	if len(coveredTypes) < 40 {
		t.Fatalf("statement branch matrix type coverage too low: got=%d want>=40", len(coveredTypes))
	}
}
