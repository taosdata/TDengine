package querygen

import (
	"strings"
	"testing"

	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

func TestGenerateQueryParseRate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(42)
	const total = 400
	ok := 0
	for i := 0; i < total; i++ {
		out, err := g.Next(r)
		if err != nil {
			t.Fatalf("generate failed: %v", err)
		}
		res := parsergate.Parse(out.SQL)
		if res.Err == nil {
			ok++
		}
	}
	if ok < 180 {
		t.Fatalf("parse success too low: %d/%d", ok, total)
	}
}

func TestGenerateQueryCombos(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(7)
	seen := map[string]struct{}{}
	for i := 0; i < 1200; i++ {
		out, err := g.Next(r)
		if err != nil {
			t.Fatalf("generate failed: %v", err)
		}
		for _, tag := range out.Tags {
			seen[tag] = struct{}{}
		}
	}
	want := []string{"query_expression", "query_specification", "join", "subquery", "window", "fill", "group_by", "order_by", "insert_query", "union_query_expression"}
	for _, k := range want {
		if _, ok := seen[k]; !ok {
			t.Fatalf("missing combo tag: %s (seen=%v)", k, seen)
		}
	}
}

func TestBindSchema(t *testing.T) {
	g := New(DefaultConfig())
	g.BindSchema(Schema{
		Tables: []Table{
			{
				Name: "meters",
				Columns: []Column{
					{Name: "ts", Type: "timestamp"},
					{Name: "current", Type: "double"},
					{Name: "ok", Type: "bool"},
					{Name: "note", Type: "varchar"},
					{Name: "payload", Type: "json"},
				},
			},
		},
	})
	if len(g.tables) != 1 || g.tables[0] != "meters" {
		t.Fatalf("unexpected tables: %v", g.tables)
	}
	if len(g.typedCols[kindNumber]) == 0 || len(g.typedCols[kindTime]) == 0 || len(g.typedCols[kindString]) == 0 || len(g.typedCols[kindBool]) == 0 || len(g.typedCols[kindJSON]) == 0 {
		t.Fatalf("typed columns not ready: %#v", g.typedCols)
	}
}

func TestInferKind(t *testing.T) {
	cases := map[string]valueKind{
		"timestamp":         kindTime,
		"bool":              kindBool,
		"json":              kindJSON,
		"varchar":           kindString,
		"nchar":             kindString,
		"varbinary(64)":     kindString,
		"double":            kindNumber,
		"decimal(18,6)":     kindNumber,
		"int unsigned":      kindNumber,
		"bigint unsigned":   kindNumber,
		"smallint unsigned": kindNumber,
		"tinyint unsigned":  kindNumber,
		"geometry(100)":     kindAny,
		"unknown":           kindAny,
	}
	for in, want := range cases {
		if got := inferKind(in); got != want {
			t.Fatalf("inferKind(%q)=%v want=%v", in, got, want)
		}
	}
}

func TestDefaultSchemaExpandedTypeCoverage(t *testing.T) {
	s := defaultSchema()
	if len(s.Tables) == 0 || len(s.Tables[0].Columns) == 0 {
		t.Fatalf("default schema is empty: %#v", s)
	}
	got := make(map[string]struct{}, len(s.Tables[0].Columns))
	for _, c := range s.Tables[0].Columns {
		typ := normalizeBaseType(c.Type)
		got[typ] = struct{}{}
	}

	want := []string{
		"timestamp",
		"int",
		"int unsigned",
		"bigint",
		"bigint unsigned",
		"float",
		"double",
		"smallint",
		"smallint unsigned",
		"tinyint",
		"tinyint unsigned",
		"bool",
		"binary",
		"varchar",
		"nchar",
		"varbinary",
		"geometry",
		"decimal",
	}
	for _, typ := range want {
		if _, ok := got[typ]; !ok {
			t.Fatalf("default schema missing type %q (got=%v)", typ, got)
		}
	}
}

func TestGeneratedQueryAvoidsDoubleDashAndInvalidColsAlias(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(99)
	for i := 0; i < 600; i++ {
		out, err := g.Next(r)
		if err != nil {
			t.Fatalf("generate failed: %v", err)
		}
		sql := strings.ToLower(out.SQL)
		if strings.Contains(sql, "--") {
			t.Fatalf("generated sql still contains double dash: %s", out.SQL)
		}
		if colsArgsContainAlias(sql) {
			t.Fatalf("generated cols() should not contain aliased args: %s", out.SQL)
		}
	}
}

func colsArgsContainAlias(sql string) bool {
	for i := 0; i < len(sql); {
		idx := strings.Index(sql[i:], "cols(")
		if idx < 0 {
			return false
		}
		start := i + idx + len("cols(")
		depth := 1
		j := start
		for ; j < len(sql) && depth > 0; j++ {
			switch sql[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		if depth != 0 {
			return false
		}
		args := sql[start : j-1]
		if strings.Contains(args, " as ") {
			return true
		}
		i = j
	}
	return false
}

func normalizeBaseType(typ string) string {
	t := strings.ToLower(strings.TrimSpace(typ))
	if idx := strings.IndexByte(t, '('); idx >= 0 {
		t = t[:idx]
	}
	return strings.Join(strings.Fields(t), " ")
}
