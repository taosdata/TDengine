package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type selectNegativeCase struct {
	id      string
	rule    string
	sql     string
	errType string
}

func loadSelectNegativeCases(t *testing.T) []selectNegativeCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "select_branch_negative.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open select negative matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []selectNegativeCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			t.Fatalf("invalid select negative matrix line %d: %q", line, s)
		}
		out = append(out, selectNegativeCase{
			id:      cols[0],
			rule:    cols[1],
			sql:     cols[2],
			errType: cols[3],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan select negative matrix failed: %v", err)
	}
	return out
}

func TestSelectBranchNegativeMatrix(t *testing.T) {
	cases := loadSelectNegativeCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty select negative matrix")
	}
	for _, tc := range cases {
		_, err := Parse(tc.sql)
		if err == nil {
			t.Fatalf("[%s] expected parse failure for sql=%q", tc.id, tc.sql)
		}
		if got := classifyParseErr(err); got != tc.errType {
			t.Fatalf("[%s] err type mismatch: got=%q want=%q err=%v", tc.id, got, tc.errType, err)
		}
	}
}
