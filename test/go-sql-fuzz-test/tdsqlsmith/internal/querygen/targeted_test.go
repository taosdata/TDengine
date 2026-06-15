package querygen

import (
	"strings"
	"testing"

	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

func TestNextForRulesJoinTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(11)
	out, ok, err := g.NextForRules(r, []string{"joined_table", "table_reference"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " join ") {
		t.Fatalf("expected join template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted join query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesWindowTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(12)
	out, ok, err := g.NextForRules(r, []string{"twindow_clause_opt", "fill_opt"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " interval(") || !strings.Contains(sql, " fill(") {
		t.Fatalf("expected window+fill template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted window query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesInsertTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(13)
	out, ok, err := g.NextForRules(r, []string{"insert_query"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.HasPrefix(strings.TrimSpace(sql), "insert into") {
		t.Fatalf("expected insert-query template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted insert query parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesParenthesizedJoinTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(21)
	out, ok, err := g.NextForRules(r, []string{"parenthesized_joined_table"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, "from (") || !strings.Contains(sql, " join ") {
		t.Fatalf("expected parenthesized join template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted parenthesized join parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesCountWindowTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(22)
	out, ok, err := g.NextForRules(r, []string{"count_window_args", "column_name_list", "trigger_col_name"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, "count_window(") {
		t.Fatalf("expected count_window template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted count_window parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}

func TestNextForRulesInterpFillTemplate(t *testing.T) {
	g := New(DefaultConfig())
	r := random.New(23)
	out, ok, err := g.NextForRules(r, []string{"fill_position_mode_extension", "interp_fill_mode"})
	if err != nil {
		t.Fatalf("next for rules failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected targeted query")
	}
	sql := strings.ToLower(out.SQL)
	if !strings.Contains(sql, " fill(") {
		t.Fatalf("expected interp fill template, got: %s", out.SQL)
	}
	if res := parsergate.Parse(out.SQL); res.Err != nil {
		t.Fatalf("targeted interp fill parse failed: %v, sql=%s", res.Err, out.SQL)
	}
}
