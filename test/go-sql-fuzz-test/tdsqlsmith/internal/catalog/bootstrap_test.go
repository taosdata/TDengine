package catalog

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func TestBootstrapSetupSQLIncludesAllPlannedTypes(t *testing.T) {
	sqls := BootstrapSetupSQL("tdsqlsmith_test")
	if len(sqls) < 9 {
		t.Fatalf("unexpected bootstrap sql count: %d", len(sqls))
	}
	if sqls[0] != "drop database if exists tdsqlsmith_test" {
		t.Fatalf("unexpected first bootstrap sql: %s", sqls[0])
	}
	if sqls[1] != "create database if not exists tdsqlsmith_test" {
		t.Fatalf("unexpected second bootstrap sql: %s", sqls[1])
	}
	if sqls[2] != "use tdsqlsmith_test" {
		t.Fatalf("unexpected third bootstrap sql: %s", sqls[2])
	}

	all := strings.ToLower(strings.Join(sqls, "\n"))
	want := []string{
		"timestamp",
		"int unsigned",
		"bigint unsigned",
		"float",
		"double",
		"binary(",
		"varchar(",
		"smallint unsigned",
		"tinyint unsigned",
		"bool",
		"nchar(",
		"geometry(",
		"varbinary(",
		"decimal(",
	}
	for _, token := range want {
		if !strings.Contains(all, token) {
			t.Fatalf("bootstrap SQL missing type token %q", token)
		}
	}
}

func TestDefaultSchemaTypeCoverageMatchesBootstrap(t *testing.T) {
	sqls := BootstrapSetupSQL("tdsqlsmith_test")
	if len(sqls) < 4 {
		t.Fatalf("unexpected bootstrap sql count: %d", len(sqls))
	}
	wantTypes, err := typeSetFromCreateTableSQL(sqls[3])
	if err != nil {
		t.Fatalf("extract types from bootstrap create SQL failed: %v", err)
	}

	schema := defaultSchema()
	if len(schema) == 0 || len(schema[0].Columns) == 0 {
		t.Fatalf("default schema is empty: %#v", schema)
	}
	gotTypes := typeSetFromColumns(schema[0].Columns)

	missing := setDiff(wantTypes, gotTypes)
	extra := setDiff(gotTypes, wantTypes)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("default schema type set mismatch, missing=%v extra=%v", missing, extra)
	}
}

func typeSetFromColumns(cols []Column) map[string]struct{} {
	out := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		out[normalizeBaseType(c.Type)] = struct{}{}
	}
	return out
}

func typeSetFromCreateTableSQL(sqlText string) (map[string]struct{}, error) {
	sqlText = strings.TrimSpace(sqlText)
	open := strings.IndexByte(sqlText, '(')
	close := strings.LastIndexByte(sqlText, ')')
	if open < 0 || close < 0 || close <= open {
		return nil, fmt.Errorf("invalid create table sql: %q", sqlText)
	}
	body := sqlText[open+1 : close]
	parts := splitTopLevelComma(body)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty create table columns: %q", sqlText)
	}

	out := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		fields := strings.Fields(part)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid column definition: %q", part)
		}
		typ := strings.Join(fields[1:], " ")
		out[normalizeBaseType(typ)] = struct{}{}
	}
	return out, nil
}

func splitTopLevelComma(in string) []string {
	out := make([]string, 0, 32)
	depth := 0
	start := 0
	for i, r := range in {
		switch r {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(in[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(in) {
		out = append(out, strings.TrimSpace(in[start:]))
	}
	return out
}

func normalizeBaseType(typ string) string {
	t := strings.ToLower(strings.TrimSpace(typ))
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = t[:idx]
	}
	return strings.Join(strings.Fields(t), " ")
}

func setDiff(a, b map[string]struct{}) []string {
	out := make([]string, 0, len(a))
	for k := range a {
		if _, ok := b[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
