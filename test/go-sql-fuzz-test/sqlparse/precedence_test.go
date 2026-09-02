package sqlparser

import "testing"

func parseSelectExpr(t *testing.T, sql string) Expr {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed for %q: %v", sql, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Select) != 1 {
		t.Fatalf("expected one select expr, got %d", len(sel.Select))
	}
	return sel.Select[0]
}

func requireRawExpr(t *testing.T, e Expr, kind string) *RawExpr {
	t.Helper()
	re, ok := e.(*RawExpr)
	if !ok {
		t.Fatalf("expected *RawExpr kind=%s, got %T", kind, e)
	}
	if re.Kind != kind {
		t.Fatalf("expected RawExpr kind=%s, got=%s", kind, re.Kind)
	}
	return re
}

func TestPrecedence_Arithmetic(t *testing.T) {
	// '*' must bind tighter than '+': 1 + (2 * 3)
	e := parseSelectExpr(t, "select 1 + 2 * 3;")
	add := requireRawExpr(t, e, "binary")
	if string(add.Op.Bytes) != "add" {
		t.Fatalf("expected add op, got %q", string(add.Op.Bytes))
	}
	_ = requireRawExpr(t, add.Right, "binary")
}

func TestPrecedence_Logic(t *testing.T) {
	// NOT binds tighter than AND: NOT(a > 1) AND (b > 1)
	e := parseSelectExpr(t, "select not a > 1 and b > 1;")
	andExpr := requireRawExpr(t, e, "binary")
	if string(andExpr.Op.Bytes) != "and" {
		t.Fatalf("expected and op, got %q", string(andExpr.Op.Bytes))
	}
	notExpr := requireRawExpr(t, andExpr.Left, "unary")
	if string(notExpr.Op.Bytes) != "not" {
		t.Fatalf("expected not op, got %q", string(notExpr.Op.Bytes))
	}
}

func TestPrecedence_ArrowVsCompare(t *testing.T) {
	// JSON extract should bind before comparison: (a->'x') = '1'
	e := parseSelectExpr(t, "select a->'x' = '1';")
	cmp := requireRawExpr(t, e, "cmp")
	_ = requireRawExpr(t, cmp.Left, "json")
}
