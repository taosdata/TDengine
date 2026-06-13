package sqlparser

import "testing"

func mustParseSelect(t *testing.T, sql string) *SelectStmt {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed for %q: %v", sql, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	return sel
}

func TestComplex_JoinGroupOrderLimit(t *testing.T) {
	sql := "select a.v from db1.t1 a join db1.t2 b on a.id = b.id where a.v > 10 group by a.v order by a.v desc nulls last limit 10 offset 5;"
	sel := mustParseSelect(t, sql)

	if sel.From == nil {
		t.Fatalf("expected from clause")
	}
	j, ok := sel.From.(*JoinTableExpr)
	if !ok {
		t.Fatalf("expected JoinTableExpr, got %T", sel.From)
	}
	if j.JoinType != JoinTypeInner {
		t.Fatalf("expected inner join, got %v", j.JoinType)
	}
	if sel.Where == nil {
		t.Fatalf("expected where clause")
	}
	if sel.GroupBy == nil || len(sel.GroupBy.Exprs) != 1 {
		t.Fatalf("expected one group by expr")
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Asc {
		t.Fatalf("expected one descending order by expr")
	}
	if sel.Limit == nil || string(sel.Limit.Limit.Bytes) != "10" || string(sel.Limit.Offset.Bytes) != "5" {
		t.Fatalf("expected limit/offset 10/5")
	}
}

func TestComplex_SubqueryFrom(t *testing.T) {
	sql := "select x.v from (select t.v from db1.t1 t where t.v > 1) x;"
	sel := mustParseSelect(t, sql)
	sub, ok := sel.From.(*SubqueryTableExpr)
	if !ok {
		t.Fatalf("expected SubqueryTableExpr, got %T", sel.From)
	}
	if sub.Query == nil {
		t.Fatalf("expected inner subquery")
	}
	if sub.Alias != "x" {
		t.Fatalf("expected subquery alias x, got %q", sub.Alias)
	}
}

func TestComplex_JoinVariants(t *testing.T) {
	cases := []string{
		"select a.v from t1 a left join t2 b on a.id = b.id;",
		"select a.v from t1 a right join t2 b on a.id = b.id;",
		"select a.v from t1 a full join t2 b on a.id = b.id;",
		"select a.v from t1 a left semi join t2 b on a.id = b.id;",
		"select a.v from t1 a right anti join t2 b on a.id = b.id;",
		"select a.v from t1 a left asof join t2 b on a.id = b.id;",
		"select a.v from t1 a left window join t2 b on a.id = b.id window_offset(1s, 2s);",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
	}
}

func TestComplex_SetOperations(t *testing.T) {
	cases := []string{
		"select v from t1 union all select v from t2 order by v limit 5;",
		"select v from t1 union select v from t2;",
	}
	for _, sql := range cases {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
		sel, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("expected *SelectStmt, got %T", stmt)
		}
		if sel.SetOp == "" && sel.Left == nil {
			// union with order/limit is wrapped by NewSelectStmtWithClauses around base set-op stmt.
			continue
		}
	}

	badCases := []string{
		"select v from t1 except select v from t2;",
		"select v from t1 intersect select v from t2;",
		"select v from t1 minus select v from t2;",
	}
	for _, sql := range badCases {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestComplex_WindowClauses(t *testing.T) {
	cases := []string{
		"select v from t1 interval(10s);",
		"select v from t1 interval(10s, 5s);",
		"select v from t1 session(v, 10s);",
		"select v from t1 state_window(v);",
		"select v from t1 event_window start with v > 1 end with v > 2;",
		"select v from t1 count_window(10);",
		"select v from t1 anomaly_window(v);",
		"select v from t1 anomaly_window(v, 'strict');",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
	}
}

func TestComplex_AnomalyWindowBranches(t *testing.T) {
	oneArg := mustParseSelect(t, "select v from t1 anomaly_window(v);")
	if oneArg.Window.AnomalyWindow == nil {
		t.Fatalf("expected anomaly window expr for one-arg variant")
	}
	if len(oneArg.Window.AnomalyTag.Bytes) != 0 {
		t.Fatalf("expected empty anomaly tag for one-arg variant")
	}

	twoArg := mustParseSelect(t, "select v from t1 anomaly_window(v, 'strict');")
	if twoArg.Window.AnomalyWindow == nil {
		t.Fatalf("expected anomaly window expr for two-arg variant")
	}
	if string(twoArg.Window.AnomalyTag.Bytes) != "strict" {
		t.Fatalf("expected anomaly tag strict, got %q", string(twoArg.Window.AnomalyTag.Bytes))
	}
}
