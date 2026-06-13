package sqlparser

import "testing"

func TestJsonExpr_ColumnReference(t *testing.T) {
	sel := parseSelect(t, `select v->'x' from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	j, ok := sel.Select[0].(*RawExpr)
	if !ok || j.Kind != "json" {
		t.Fatalf("expected json RawExpr, got %#v", sel.Select[0])
	}
	left, ok := j.Left.(*RawExpr)
	if !ok || left.Kind != "col" || left.Name != "v" {
		t.Fatalf("expected left column v, got %#v", j.Left)
	}
	path, ok := j.Extra.(Token)
	if !ok || string(path.Bytes) != "x" {
		t.Fatalf("expected json path x, got %#v", j.Extra)
	}
}

func TestJsonExpr_TableQualifiedColumnReference(t *testing.T) {
	sel := parseSelect(t, `select t1.v->'x' from t1;`)
	if len(sel.Select) != 1 {
		t.Fatalf("expected 1 select expr, got %d", len(sel.Select))
	}
	j, ok := sel.Select[0].(*RawExpr)
	if !ok || j.Kind != "json" {
		t.Fatalf("expected json RawExpr, got %#v", sel.Select[0])
	}
	left, ok := j.Left.(*RawExpr)
	if !ok || left.Kind != "col" || left.Name != "v" {
		t.Fatalf("expected left column v, got %#v", j.Left)
	}
	if tbl, _ := left.Extra.(string); tbl != "t1" {
		t.Fatalf("expected table qualifier t1, got %q", tbl)
	}
}

func TestJsonExpr_LeftOperandMustBeColumnReference(t *testing.T) {
	_, err := Parse(`select (v+1)->'x' from t1;`)
	if err == nil {
		t.Fatalf("expected parse error for non-column left operand")
	}
}
