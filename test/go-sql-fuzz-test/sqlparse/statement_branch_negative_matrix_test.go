package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type statementBranchNegativeCase struct {
	id         string
	stmtFamily string
	rule       string
	branchSig  string
	sql        string
	errType    string
}

func loadStatementBranchNegativeCases(t *testing.T) []statementBranchNegativeCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "statement_branch_negative.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open statement branch negative matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []statementBranchNegativeCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 6 {
			t.Fatalf("invalid statement branch negative line %d: %q", line, s)
		}
		out = append(out, statementBranchNegativeCase{
			id:         cols[0],
			stmtFamily: cols[1],
			rule:       cols[2],
			branchSig:  cols[3],
			sql:        cols[4],
			errType:    cols[5],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan statement branch negative failed: %v", err)
	}
	return out
}

func TestStatementBranchNegativeMatrix(t *testing.T) {
	cases := loadStatementBranchNegativeCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty statement branch negative matrix")
	}

	errTypeHit := map[string]int{}
	for _, tc := range cases {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Fatalf("[%s] expected parse failure, sql=%q", tc.id, tc.sql)
		}
		gotType := classifyParseErr(err)
		errTypeHit[gotType]++
		if gotType != tc.errType {
			t.Fatalf("[%s] invalid err type: got=%q want=%q err=%v sql=%q", tc.id, gotType, tc.errType, err, tc.sql)
		}
	}

	for _, et := range []string{"syntax", "incomplete"} {
		if errTypeHit[et] == 0 {
			t.Fatalf("negative matrix missing error type branch: %s", et)
		}
	}
}
