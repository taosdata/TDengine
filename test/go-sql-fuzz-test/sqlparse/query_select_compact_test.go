package sqlparser

import "testing"

func parseSelect(t *testing.T, sql string) *SelectStmt {
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

func extractJoinFromSQL(t *testing.T, sql string) *JoinTableExpr {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	j, ok := sel.From.(*JoinTableExpr)
	if !ok {
		t.Fatalf("expected JoinTableExpr, got %T", sel.From)
	}
	return j
}

func parseWindow(t *testing.T, sql string) WindowExpr {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed for %q: %v", sql, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	return sel.Window
}

func parsePartitionArgs(t *testing.T, sql string) []Expr {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed for %q: %v", sql, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	part, ok := sel.Partition.(*RawExpr)
	if !ok || part.Kind != "partition_by" {
		t.Fatalf("expected partition_by expr, got %#v", sel.Partition)
	}
	return part.Args
}

func TestSelectColumnAlias_QuotedAliasToken(t *testing.T) {
	sel := parseSelect(t, `select a "x" from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	aliased, ok := sel.Select[0].(*AliasedExpr)
	if !ok {
		t.Fatalf("expected *AliasedExpr, got %T", sel.Select[0])
	}
	if aliased.Alias != "x" {
		t.Fatalf("expected alias x, got %q", aliased.Alias)
	}
}

func TestColumnReference_QuotedIdentifier(t *testing.T) {
	sel := parseSelect(t, `select "c1" from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	col, ok := sel.Select[0].(*RawExpr)
	if !ok {
		t.Fatalf("expected *RawExpr, got %T", sel.Select[0])
	}
	if col.Kind != "col" || col.Name != "c1" {
		t.Fatalf("unexpected column expr: %#v", col)
	}
	if tbl, _ := col.Extra.(string); tbl != "" {
		t.Fatalf("expected empty table qualifier, got %q", tbl)
	}
}

func TestColumnReference_TableQualifiedQuotedIdentifier(t *testing.T) {
	sel := parseSelect(t, `select t1."c1" from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	col, ok := sel.Select[0].(*RawExpr)
	if !ok {
		t.Fatalf("expected *RawExpr, got %T", sel.Select[0])
	}
	if col.Kind != "col" || col.Name != "c1" {
		t.Fatalf("unexpected column expr: %#v", col)
	}
	if tbl, _ := col.Extra.(string); tbl != "t1" {
		t.Fatalf("expected table qualifier t1, got %q", tbl)
	}
}

func TestQuery_RangeBranches(t *testing.T) {
	s1 := parseSelect(t, "select v from t1 range(v);")
	r1, ok := s1.Range.(*RawExpr)
	if !ok || r1.Kind != "range_1" || len(r1.Args) != 1 {
		t.Fatalf("unexpected range_1: %#v", s1.Range)
	}

	s2 := parseSelect(t, "select v from t1 range(v, v);")
	r2, ok := s2.Range.(*RawExpr)
	if !ok || r2.Kind != "range_2" || len(r2.Args) != 2 {
		t.Fatalf("unexpected range_2: %#v", s2.Range)
	}

	s3 := parseSelect(t, "select v from t1 range(v, v, v);")
	r3, ok := s3.Range.(*RawExpr)
	if !ok || r3.Kind != "range_3" || len(r3.Args) != 3 {
		t.Fatalf("unexpected range_3: %#v", s3.Range)
	}
}

func TestQuery_EveryAndInterpFillBranches(t *testing.T) {
	s1 := parseSelect(t, "select v from t1;")
	if len(s1.Every.Val.Bytes) != 0 || s1.InterpFill != nil {
		t.Fatalf("unexpected empty branches: %+v", s1)
	}

	s2 := parseSelect(t, "select v from t1 every(10s) fill(prev);")
	if string(s2.Every.Val.Bytes) != "10s" || s2.InterpFill == nil || s2.InterpFill.Mode == nil || s2.InterpFill.Mode.Name != "prev" {
		t.Fatalf("unexpected every/fill: %+v", s2)
	}

	s3 := parseSelect(t, "select v from t1 fill(near, 1);")
	if s3.InterpFill == nil || s3.InterpFill.Mode == nil || s3.InterpFill.Mode.Name != "near" || len(s3.InterpFill.Values) != 1 {
		t.Fatalf("unexpected near fill branch: %+v", s3)
	}

	s4 := parseSelect(t, "select v from t1 fill(value, 1, 2);")
	if s4.InterpFill == nil || s4.InterpFill.Mode == nil || s4.InterpFill.Mode.Name != "value" || len(s4.InterpFill.Values) != 2 {
		t.Fatalf("unexpected value fill branch: %+v", s4)
	}
}

func TestPartitionBy_ItemBranches(t *testing.T) {
	args1 := parsePartitionArgs(t, "select v from t1 partition by c1;")
	if len(args1) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args1))
	}
	if _, ok := args1[0].(*AliasedExpr); ok {
		t.Fatalf("expected plain expr for args1[0], got aliased")
	}

	args2 := parsePartitionArgs(t, "select v from t1 partition by c1 p1;")
	a2, ok := args2[0].(*AliasedExpr)
	if len(args2) != 1 || !ok || a2.Alias != "p1" {
		t.Fatalf("unexpected args2: %#v", args2)
	}

	args3 := parsePartitionArgs(t, "select v from t1 partition by c1 as p1, c2 p2;")
	if len(args3) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args3))
	}
	a3_0, ok0 := args3[0].(*AliasedExpr)
	a3_1, ok1 := args3[1].(*AliasedExpr)
	if !ok0 || !ok1 || a3_0.Alias != "p1" || a3_1.Alias != "p2" {
		t.Fatalf("unexpected args3: %#v", args3)
	}
}

func TestFunction_PositionExpr(t *testing.T) {
	stmt, err := Parse("select position(a in b) from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	re, ok := sel.Select[0].(*RawExpr)
	if !ok {
		t.Fatalf("expected *RawExpr, got %T", sel.Select[0])
	}
	if re.Kind != "position" {
		t.Fatalf("expected position expr kind, got %q", re.Kind)
	}
}

func TestFunction_TrimExtendedBranches(t *testing.T) {
	cases := []struct {
		sql  string
		spec string
	}{
		{"select trim(a from b) from t1;", ""},
		{"select trim(leading a from b) from t1;", "leading"},
	}
	for _, tt := range cases {
		stmt, err := Parse(tt.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tt.sql, err)
		}
		sel, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("expected *SelectStmt, got %T", stmt)
		}
		re, ok := sel.Select[0].(*RawExpr)
		if !ok {
			t.Fatalf("expected *RawExpr, got %T", sel.Select[0])
		}
		if re.Kind != "trim_ext" {
			t.Fatalf("expected trim_ext kind, got %q", re.Kind)
		}
		if spec, _ := re.Extra.(string); spec != tt.spec {
			t.Fatalf("expected trim spec %q, got %#v", tt.spec, re.Extra)
		}
	}
}

func TestCountWindow_ArgBranches(t *testing.T) {
	w1 := parseWindow(t, "select v from t1 count_window(10);")
	if string(w1.CountWindow.Bytes) != "10" || len(w1.CountWindowSlide.Bytes) != 0 || len(w1.CountWindowCols) != 0 {
		t.Fatalf("unexpected w1: %+v", w1)
	}

	w2 := parseWindow(t, "select v from t1 count_window(10, 2);")
	if string(w2.CountWindow.Bytes) != "10" || string(w2.CountWindowSlide.Bytes) != "2" || len(w2.CountWindowCols) != 0 {
		t.Fatalf("unexpected w2: %+v", w2)
	}

	w3 := parseWindow(t, "select v from t1 count_window(10, c1, c2);")
	if string(w3.CountWindow.Bytes) != "10" || len(w3.CountWindowSlide.Bytes) != 0 || len(w3.CountWindowCols) != 2 {
		t.Fatalf("unexpected w3: %+v", w3)
	}

	w4 := parseWindow(t, "select v from t1 count_window(10, 2, c1, c2);")
	if string(w4.CountWindow.Bytes) != "10" || string(w4.CountWindowSlide.Bytes) != "2" || len(w4.CountWindowCols) != 2 {
		t.Fatalf("unexpected w4: %+v", w4)
	}
}

func TestEventWindow_TrueForBranches(t *testing.T) {
	w1 := parseWindow(t, "select v from t1 event_window start with v > 1 end with v > 2;")
	if w1.EventWindowStart == nil || w1.EventWindowEnd == nil || len(w1.TrueFor.Val.Bytes) != 0 {
		t.Fatalf("unexpected w1: %+v", w1)
	}

	w2 := parseWindow(t, "select v from t1 event_window start with v > 1 end with v > 2 true_for(5s);")
	if string(w2.TrueFor.Val.Bytes) != "5s" {
		t.Fatalf("expected true_for 5s, got %+v", w2.TrueFor)
	}
}

func TestEventWindow_SearchConditionListBranches_RejectInSelect(t *testing.T) {
	cases := []string{
		"select v from t1 event_window(start with (v > 1, v < 10) end with v > 2) true_for(5s);",
		"select v from t1 event_window(start with (v > 1, v < 10)) true_for(3s);",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse failure for %q", sql)
		}
	}
}

func TestIntervalWindow_SlidingAndFillBranches(t *testing.T) {
	w1 := parseWindow(t, "select v from t1 interval(10s);")
	if string(w1.Interval.Val.Bytes) != "10s" || len(w1.Sliding.Val.Bytes) != 0 || w1.Fill != nil {
		t.Fatalf("unexpected w1: %+v", w1)
	}

	w2 := parseWindow(t, "select v from t1 interval(10s) sliding(5s);")
	if string(w2.Interval.Val.Bytes) != "10s" || string(w2.Sliding.Val.Bytes) != "5s" || w2.Fill != nil {
		t.Fatalf("unexpected w2: %+v", w2)
	}

	w3 := parseWindow(t, "select v from t1 interval(10s, 1s) fill(null);")
	if string(w3.Interval.Val.Bytes) != "10s" || string(w3.Offset.Val.Bytes) != "1s" || w3.Fill == nil || w3.Fill.Mode == nil || w3.Fill.Mode.Name != "null" {
		t.Fatalf("unexpected w3: %+v", w3)
	}

	w4 := parseWindow(t, "select v from t1 interval(10s, auto) sliding(2s);")
	if string(w4.Interval.Val.Bytes) != "10s" || string(w4.Offset.Val.Bytes) != "auto" || string(w4.Sliding.Val.Bytes) != "2s" {
		t.Fatalf("unexpected w4: %+v", w4)
	}

	w5 := parseWindow(t, "select v from t1 interval(10s) fill(value_f, 1);")
	if w5.Fill == nil || w5.Fill.Mode == nil || w5.Fill.Mode.Name != "value_f" || len(w5.Fill.Values) != 1 {
		t.Fatalf("unexpected w5: %+v", w5)
	}
}

func TestPeriodWindow_OffsetOptBranches_RejectInSelect(t *testing.T) {
	cases := []string{
		"select v from t1 period(10s);",
		"select v from t1 period(10s, 1s);",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse failure for %q", sql)
		}
	}
}

func TestWindow_SessionTwoArgs(t *testing.T) {
	stmt, err := Parse("select v from t1 session(v, 10s);")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if sel.Window.Session == nil {
		t.Fatalf("expected session expr")
	}
	if string(sel.Window.SessionGap.Val.Bytes) != "10s" {
		t.Fatalf("expected session gap 10s, got %q", string(sel.Window.SessionGap.Val.Bytes))
	}
}

func TestStateWindow_OptionAndTrueForBranches(t *testing.T) {
	w1 := parseWindow(t, "select v from t1 state_window(v);")
	if w1.StateWindow == nil || w1.StateWindowOpt.HasExtend || len(w1.TrueFor.Val.Bytes) != 0 {
		t.Fatalf("unexpected w1: %+v", w1)
	}

	w2 := parseWindow(t, "select v from t1 state_window(v, 1);")
	if !w2.StateWindowOpt.HasExtend || string(w2.StateWindowOpt.Extend.Val.Bytes) != "1" || w2.StateWindowOpt.HasZeroth {
		t.Fatalf("unexpected w2: %+v", w2.StateWindowOpt)
	}

	w3 := parseWindow(t, "select v from t1 state_window(v, 1, 'z');")
	if !w3.StateWindowOpt.HasExtend || !w3.StateWindowOpt.HasZeroth || string(w3.StateWindowOpt.Zeroth.Val.Bytes) != "z" {
		t.Fatalf("unexpected w3: %+v", w3.StateWindowOpt)
	}

	w4 := parseWindow(t, "select v from t1 state_window(v) true_for(10s);")
	if string(w4.TrueFor.Val.Bytes) != "10s" {
		t.Fatalf("expected true_for 10s, got %+v", w4.TrueFor)
	}

	w5 := parseWindow(t, "select v from t1 state_window(v, 1, -2);")
	if !w5.StateWindowOpt.HasZeroth || string(w5.StateWindowOpt.Zeroth.Val.Bytes) != "-2" {
		t.Fatalf("unexpected w5 zeroth: %+v", w5.StateWindowOpt)
	}

	w6 := parseWindow(t, "select v from t1 state_window(v, 1, +2);")
	if !w6.StateWindowOpt.HasZeroth || string(w6.StateWindowOpt.Zeroth.Val.Bytes) != "2" {
		t.Fatalf("unexpected w6 zeroth: %+v", w6.StateWindowOpt)
	}
}

func TestAsofJoin_JLimitBranches(t *testing.T) {
	withJLimit := extractJoinFromSQL(t, "select a.v from t1 a left asof join t2 b on a.id = b.id jlimit 10;")
	if withJLimit.JLimit == nil || string(withJLimit.JLimit.Limit.Bytes) != "10" {
		t.Fatalf("expected jlimit=10, got %+v", withJLimit.JLimit)
	}

	withoutJLimit := extractJoinFromSQL(t, "select a.v from t1 a left asof join t2 b on a.id = b.id;")
	if withoutJLimit.JLimit != nil {
		t.Fatalf("expected nil jlimit, got %+v", withoutJLimit.JLimit)
	}
}

func TestWindowJoin_WindowOffsetAndJLimit(t *testing.T) {
	j := extractJoinFromSQL(t, "select a.v from t1 a right window join t2 b on a.id = b.id window_offset(1s, -2s) jlimit 3;")
	if j.WindowOffset == nil {
		t.Fatalf("expected window offset")
	}
	if j.JLimit == nil || string(j.JLimit.Limit.Bytes) != "3" {
		t.Fatalf("expected jlimit=3, got %+v", j.JLimit)
	}

	noJ := extractJoinFromSQL(t, "select a.v from t1 a right window join t2 b on a.id = b.id window_offset(1s, -2s);")
	if noJ.WindowOffset == nil {
		t.Fatalf("expected window offset without jlimit")
	}
	if noJ.JLimit != nil {
		t.Fatalf("expected nil jlimit, got %+v", noJ.JLimit)
	}
}

func TestLimitPlaceholderBranches(t *testing.T) {
	s1 := parseSelect(t, "select v from t1 limit ?;")
	if s1.Limit == nil || string(s1.Limit.Limit.Bytes) != "?" {
		t.Fatalf("unexpected limit ?: %+v", s1.Limit)
	}

	s2 := parseSelect(t, "select v from t1 slimit ?, ?;")
	if s2.SLimit == nil || string(s2.SLimit.SLimit.Bytes) != "?" || string(s2.SLimit.SOffset.Bytes) != "?" {
		t.Fatalf("unexpected slimit ?: %+v", s2.SLimit)
	}

	j := extractJoinFromSQL(t, "select a.v from t1 a left asof join t2 b on a.id = b.id jlimit ?;")
	if j.JLimit == nil || string(j.JLimit.Limit.Bytes) != "?" {
		t.Fatalf("unexpected jlimit ?: %+v", j.JLimit)
	}
}

func TestScalarSubqueryExpr_SelectItem(t *testing.T) {
	stmt, err := Parse("select (select v from t2) from t1;")
	if err != nil {
		t.Fatalf("parse scalar subquery select item failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Select) != 1 {
		t.Fatalf("expected one select item, got %d", len(sel.Select))
	}
	if _, ok := sel.Select[0].(*SelectStmt); !ok {
		t.Fatalf("expected scalar subquery expression, got %T", sel.Select[0])
	}
}

func TestInPredicate_LiteralListSignedBranches(t *testing.T) {
	stmt, err := Parse("select v from t1 where v in (-1, +2, -3.5, 'x');")
	if err != nil {
		t.Fatalf("parse in literal list failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	inExpr, ok := sel.Where.(*RawExpr)
	if !ok || inExpr.Kind != "in" {
		t.Fatalf("expected in predicate expr, got %#v", sel.Where)
	}
	if len(inExpr.Args) != 4 {
		t.Fatalf("expected 4 in-list args, got %#v", inExpr.Args)
	}
}

func TestSelectStatement_QueryExpressionBranch(t *testing.T) {
	stmt, err := Parse("select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if _, ok := stmt.(*SelectStmt); !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
}

func TestSelectStatement_SubqueryBranch(t *testing.T) {
	stmt, err := Parse("(select v from t1);")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if _, ok := stmt.(*SelectStmt); !ok {
		t.Fatalf("expected *SelectStmt from subquery branch, got %T", stmt)
	}
}

func TestParenthesizedJoin_SingleLevel(t *testing.T) {
	sql := "select a.v from (t1 a join t2 b on a.id = b.id);"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if _, ok := sel.From.(*JoinTableExpr); !ok {
		t.Fatalf("expected from to be JoinTableExpr, got %T", sel.From)
	}
}

func TestParenthesizedJoin_NestedLevel(t *testing.T) {
	sql := "select a.v from ((t1 a join t2 b on a.id = b.id));"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if _, ok := sel.From.(*JoinTableExpr); !ok {
		t.Fatalf("expected nested from to be JoinTableExpr, got %T", sel.From)
	}
}
