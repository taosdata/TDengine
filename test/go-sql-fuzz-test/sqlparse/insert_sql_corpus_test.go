package sqlparser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadInsertSQLCases(t *testing.T) []string {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "insert_sql_cases.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open insert sql corpus failed: %v", err)
	}
	defer f.Close()

	out := make([]string, 0, 512)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan insert sql corpus failed: %v", err)
	}
	return out
}

func TestInsertSQLCorpus_ParseAll(t *testing.T) {
	sqls := loadInsertSQLCases(t)
	if len(sqls) < 500 {
		t.Fatalf("expected at least 500 insert sql cases, got %d", len(sqls))
	}

	var insertStmtCount int
	for i, sql := range sqls {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse insert sql failed at #%d: %v, sql=%q", i+1, err, sql)
		}
		if _, ok := stmt.(InsertStatement); !ok {
			t.Fatalf("expected InsertStatement at #%d, got %T, sql=%q", i+1, stmt, sql)
		}
		insertStmtCount++
	}
	if insertStmtCount == 0 {
		t.Fatalf("no InsertStatement parsed from insert corpus")
	}
}

func TestInsertParse_DBTableColumnsNowInt(t *testing.T) {
	sql := "insert into db.tb (ts,v) values(now,123);"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ins, ok := stmt.(InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if len(ins) != 1 {
		t.Fatalf("expected single insert node, got %d", len(ins))
	}
	n := ins[0]
	if n.TableName == nil || n.TableName.Qualifier.String() != "db" || n.TableName.Name.String() != "tb" {
		t.Fatalf("unexpected table name: %+v", n.TableName)
	}
	if len(n.Fields) != 2 || string(n.Fields[0]) != "ts" || string(n.Fields[1]) != "v" {
		t.Fatalf("unexpected fields: %+v", n.Fields)
	}
	if len(n.Values) != 1 || len(n.Values[0]) != 2 {
		t.Fatalf("unexpected value rows: %+v", n.Values)
	}
	if n.Values[0][0] == nil || n.Values[0][0].Type != TimeVal || string(n.Values[0][0].Val) != "now" {
		t.Fatalf("unexpected first value: %+v", n.Values[0][0])
	}
	if n.Values[0][1] == nil || n.Values[0][1].Type != IntVal || string(n.Values[0][1].Val) != "123" {
		t.Fatalf("unexpected second value: %+v", n.Values[0][1])
	}
}

func TestInsertSQLCorpus_RoundTripSample(t *testing.T) {
	sqls := loadInsertSQLCases(t)
	if len(sqls) == 0 {
		t.Fatalf("no insert sql loaded")
	}

	// Sample every 20th SQL to keep this test fast while checking format/parse stability.
	for i := 0; i < len(sqls); i += 20 {
		sql := sqls[i]
		t.Run(fmt.Sprintf("insert_%03d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}
