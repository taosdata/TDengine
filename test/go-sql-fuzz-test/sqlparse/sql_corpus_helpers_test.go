package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type validSQLCase struct {
	id        string
	sql       string
	stmtType  string
	keyAssert string
}

type invalidSQLCase struct {
	id      string
	sql     string
	errType string
}

func loadValidSQLCases(t *testing.T) []validSQLCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "valid_sql_cases.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open valid sql corpus failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []validSQLCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			t.Fatalf("invalid valid sql corpus line %d: %q", line, s)
		}
		out = append(out, validSQLCase{
			id:        cols[0],
			sql:       cols[1],
			stmtType:  cols[2],
			keyAssert: cols[3],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan valid sql corpus failed: %v", err)
	}
	return out
}

func loadInvalidSQLCases(t *testing.T) []invalidSQLCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "invalid_sql_cases.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open invalid sql corpus failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []invalidSQLCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 3 {
			t.Fatalf("invalid invalid sql corpus line %d: %q", line, s)
		}
		out = append(out, invalidSQLCase{
			id:      cols[0],
			sql:     cols[1],
			errType: cols[2],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan invalid sql corpus failed: %v", err)
	}
	return out
}

func classifyParseErr(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "incomplete sql") {
		return "incomplete"
	}
	if strings.Contains(msg, "syntax error") {
		return "syntax"
	}
	return "lexical"
}

func assertKeyFields(t *testing.T, stmt Statement, keySpec string) {
	t.Helper()
	if keySpec == "" {
		return
	}
	parts := strings.Split(keySpec, ";")
	for _, p := range parts {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			t.Fatalf("invalid key assertion %q", p)
		}
		k, v := kv[0], kv[1]
		if k == "roundtrip" {
			// Metadata-only key used by corpus generators to flag non-idempotent
			// formatting branches that should not participate in round-trip gates.
			_ = v
			continue
		}
		switch s := stmt.(type) {
		case *ShowStmt:
			switch k {
			case "kind":
				if s.Kind != v {
					t.Fatalf("show kind mismatch: got=%q want=%q", s.Kind, v)
				}
			case "dbname":
				if s.DBName != v {
					t.Fatalf("show dbname mismatch: got=%q want=%q", s.DBName, v)
				}
			case "id":
				got := strconv.FormatInt(int64(s.ID), 10)
				if got != v {
					t.Fatalf("show id mismatch: got=%q want=%q", got, v)
				}
			case "hasid":
				want := v == "true"
				if s.HasID != want {
					t.Fatalf("show hasid mismatch: got=%v want=%v", s.HasID, want)
				}
			default:
				t.Fatalf("unsupported show key %q", k)
			}
		case *CreateDatabaseStmt:
			switch k {
			case "dbname":
				if s.DbName != v {
					t.Fatalf("create database name mismatch: got=%q want=%q", s.DbName, v)
				}
			case "ignoreexists":
				want := v == "true"
				if s.IgnoreExists != want {
					t.Fatalf("create database ignoreexists mismatch: got=%v want=%v", s.IgnoreExists, want)
				}
			default:
				t.Fatalf("unsupported create database key %q", k)
			}
		case *AlterDatabaseStmt:
			switch k {
			case "name":
				if s.Name != v {
					t.Fatalf("alter database name mismatch: got=%q want=%q", s.Name, v)
				}
			case "buffer":
				if s.Options == nil {
					t.Fatalf("alter database options nil")
				}
				want, err := strconv.Atoi(v)
				if err != nil {
					t.Fatalf("invalid expected buffer value %q: %v", v, err)
				}
				if s.Options.Buffer != int32(want) {
					t.Fatalf("alter database buffer mismatch: got=%d want=%d", s.Options.Buffer, want)
				}
			default:
				t.Fatalf("unsupported alter database key %q", k)
			}
		case *CreateTableStmt:
			switch k {
			case "table":
				if s.TableName == nil || s.TableName.Name.String() != v {
					got := ""
					if s.TableName != nil {
						got = s.TableName.Name.String()
					}
					t.Fatalf("create table name mismatch: got=%q want=%q", got, v)
				}
			case "is_stable":
				want := v == "true"
				if s.IsStable != want {
					t.Fatalf("create table is_stable mismatch: got=%v want=%v", s.IsStable, want)
				}
			case "tags_len":
				want, err := strconv.Atoi(v)
				if err != nil {
					t.Fatalf("invalid expected tags_len %q: %v", v, err)
				}
				if len(s.Tags) != want {
					t.Fatalf("create table tags_len mismatch: got=%d want=%d", len(s.Tags), want)
				}
			case "options_non_nil":
				want := v == "true"
				got := s.Options != nil
				if got != want {
					t.Fatalf("create table options_non_nil mismatch: got=%v want=%v", got, want)
				}
			default:
				t.Fatalf("unsupported create table key %q", k)
			}
		default:
			t.Fatalf("unsupported statement type for key assertions: %T", stmt)
		}
	}
}
