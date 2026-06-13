package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sqlparser"
)

var sqlFieldRE = regexp.MustCompile(`sql:\s*"((?:[^"\\]|\\.)*)"`)

func normalizeStmtType(t string) string {
	t = strings.TrimSpace(t)
	t = strings.TrimPrefix(t, "*")
	t = strings.TrimPrefix(t, "sqlparser.")
	return t
}

func addSQL(set map[string]struct{}, sql string) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return
	}
	set[sql] = struct{}{}
}

func loadFromTSV(path string, col int, set map[string]struct{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		if line == 1 {
			continue
		}
		cols := strings.Split(sc.Text(), "\t")
		if len(cols) <= col {
			continue
		}
		addSQL(set, cols[col])
	}
	return sc.Err()
}

func loadFromCommandTests(path string, set map[string]struct{}) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, m := range sqlFieldRE.FindAllStringSubmatch(string(b), -1) {
		if len(m) != 2 {
			continue
		}
		s := strings.ReplaceAll(m[1], `\"`, `"`)
		s = strings.ReplaceAll(s, `\\`, `\\`)
		addSQL(set, s)
	}
	return nil
}

func isRoundTripStable(sql string) bool {
	stmt, err := sqlparser.Parse(sql)
	if err != nil || stmt == nil {
		return false
	}
	tb := &sqlparser.TrackedBuffer{Buffer: &bytes.Buffer{}}
	stmt.Format(tb)
	out := strings.TrimSpace(tb.String())
	if out == "" {
		return false
	}
	_, err = sqlparser.Parse(out)
	return err == nil
}

func main() {
	root := "."
	set := map[string]struct{}{}

	must := func(err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	must(loadFromTSV(filepath.Join(root, "testdata", "sql_corpus", "valid_sql_cases.tsv"), 1, set))
	must(loadFromTSV(filepath.Join(root, "testdata", "sql_corpus", "statement_branch_matrix.tsv"), 4, set))
	must(loadFromTSV(filepath.Join(root, "testdata", "sql_corpus", "select_branch_matrix.tsv"), 3, set))
	must(loadFromCommandTests(filepath.Join(root, "command_entry_statement_test.go"), set))
	must(loadFromCommandTests(filepath.Join(root, "command_matrix_test.go"), set))

	// Ensure currently missing statement families in statement branch matrix each have at least one SQL seed.
	for _, s := range []string{
		"drop anode 1;",
		"drop dnode node1;",
		"drop role if exists r1;",
		"flush database db1;",
	} {
		addSQL(set, s)
	}

	sqls := make([]string, 0, len(set))
	for s := range set {
		sqls = append(sqls, s)
	}
	sort.Strings(sqls)

	type row struct {
		id       string
		sql      string
		stmtType string
		key      string
	}
	rows := make([]row, 0, len(sqls))
	for _, s := range sqls {
		stmt, err := sqlparser.Parse(s)
		if err != nil {
			continue
		}
		r := row{sql: s, stmtType: normalizeStmtType(fmt.Sprintf("%T", stmt))}
		if !isRoundTripStable(s) {
			r.key = "roundtrip=false"
		}
		rows = append(rows, r)
	}

	outPath := filepath.Join(root, "testdata", "sql_corpus", "valid_sql_cases.tsv")
	f, err := os.Create(outPath)
	must(err)
	defer f.Close()

	w := bufio.NewWriter(f)
	_, _ = fmt.Fprintln(w, "case_id\tsql\tstmt_type\tkey_assert")
	for i, r := range rows {
		r.id = fmt.Sprintf("v%04d", i+1)
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.id, r.sql, r.stmtType, r.key)
	}
	must(w.Flush())

	fmt.Printf("generated %d valid sql cases -> %s\n", len(rows), outPath)
}
