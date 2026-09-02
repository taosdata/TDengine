package sqlparser

import (
	"bytes"
	"encoding/json"
	"testing"
)

func newTB() *TrackedBuffer {
	return &TrackedBuffer{Buffer: &bytes.Buffer{}}
}

func TestExprNodes_AliasColStar(t *testing.T) {
	from := &RawExpr{Kind: "col", Name: "a"}
	to := &RawExpr{Kind: "col", Name: "b"}
	al := &AliasedExpr{Expr: from, Alias: "x"}
	if !al.replace(from, to) {
		t.Fatalf("expected replace success")
	}
	if al.Expr != to {
		t.Fatalf("replace target not applied")
	}
	if al.replace(from, to) {
		t.Fatalf("unexpected replace success on stale source")
	}

	tb := newTB()
	al.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected formatted alias output")
	}
	if err := al.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("walkSubtree failed: %v", err)
	}

	c := NewColIdent("AbC")
	if c.Lowered() != "abc" || !c.EqualString("ABC") || c.IsEmpty() {
		t.Fatalf("unexpected colident behavior: %+v", c)
	}
	tb2 := newTB()
	c.Format(tb2)
	if tb2.String() == "" {
		t.Fatalf("expected colident format output")
	}
	if err := c.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("colident walk failed: %v", err)
	}

	s := &StarExpr{TableName: "t1"}
	tb3 := newTB()
	s.Format(tb3)
	if tb3.String() != "t1.*" {
		t.Fatalf("unexpected star format: %q", tb3.String())
	}
	if s.replace(from, to) {
		t.Fatalf("star replace should always be false")
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("star walk failed: %v", err)
	}
}

func TestExprNodes_AliasedExprNilBranches(t *testing.T) {
	var nilAlias *AliasedExpr
	nilTB := newTB()
	nilAlias.Format(nilTB)
	if nilTB.String() != "" {
		t.Fatalf("nil alias format should be empty, got=%q", nilTB.String())
	}
	if nilAlias.replace(&RawExpr{Kind: "col", Name: "a"}, &RawExpr{Kind: "col", Name: "b"}) {
		t.Fatalf("nil alias replace should be false")
	}
	if err := nilAlias.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil alias walk should not fail: %v", err)
	}

	withEmptyExpr := &AliasedExpr{}
	tbEmpty := newTB()
	withEmptyExpr.Format(tbEmpty)
	if tbEmpty.String() != "" {
		t.Fatalf("empty expr alias format should be empty, got=%q", tbEmpty.String())
	}
	if withEmptyExpr.replace(&RawExpr{Kind: "col", Name: "a"}, &RawExpr{Kind: "col", Name: "b"}) {
		t.Fatalf("empty expr replace should be false")
	}

	withAs := &AliasedExpr{Expr: &RawExpr{Kind: "col", Name: "a"}, As: NewColIdent("alias_name")}
	tb := newTB()
	withAs.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected non-empty format output")
	}
}

func TestExprNodes_ColIdentBranches(t *testing.T) {
	c := NewColIdent("A1-b")
	if c.String() != "A1-b" {
		t.Fatalf("unexpected String(): %q", c.String())
	}
	if c.CompliantName() != "A1_b" {
		t.Fatalf("unexpected CompliantName(): %q", c.CompliantName())
	}
	if !c.Equal(NewColIdent("a1-B")) {
		t.Fatalf("Equal should be case-insensitive")
	}
	if c.Equal(NewColIdent("x")) {
		t.Fatalf("Equal should fail for different values")
	}
	if c.Lowered() != "a1-b" {
		t.Fatalf("unexpected Lowered(): %q", c.Lowered())
	}
	if NewColIdent("").Lowered() != "" {
		t.Fatalf("empty Lowered() should be empty")
	}

	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out ColIdent
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if out.String() != c.String() {
		t.Fatalf("json round trip mismatch: %q vs %q", out.String(), c.String())
	}

	var bad ColIdent
	if err := bad.UnmarshalJSON([]byte("{")); err == nil {
		t.Fatalf("expected unmarshal error for invalid json")
	}
}

func TestStmtSelectStub_Helpers(t *testing.T) {
	inList := &RawExpr{Kind: "in_list", Args: []Expr{Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}}}
	_ = NewInPredicateExpr(nil, &RawExpr{Kind: "col", Name: "v"}, OP_TYPE_IN, inList)

	sub := &SelectStmt{SetOp: "union"}
	_ = NewInPredicateExpr(nil, &RawExpr{Kind: "col", Name: "v"}, OP_TYPE_IN, sub)
	_ = NewInPredicateExpr(nil, &RawExpr{Kind: "col", Name: "v"}, OP_TYPE_IN, &RawExpr{Kind: "other"})

	w := NewIntervalAutoWindowExpr(nil, Literal{Val: Token{Bytes: []byte("10s")}, Type: LiteralDuration}, Token{Bytes: []byte("auto")}, Literal{}, nil)
	if string(w.Offset.Val.Bytes) != "auto" {
		t.Fatalf("unexpected auto offset: %+v", w)
	}

	tn := &TableNameExpr{DBName: "db", TableName: "t", Alias: "a"}
	tb := newTB()
	tn.Format(tb)
	if err := tn.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("tablename walk failed: %v", err)
	}

	sq := &SubqueryTableExpr{Query: &SelectStmt{}, Alias: "x"}
	tb2 := newTB()
	sq.Format(tb2)
	if err := sq.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("subquery walk failed: %v", err)
	}

	j := &JoinTableExpr{Left: tn, Right: sq, JoinType: JoinTypeInner}
	tb3 := newTB()
	j.Format(tb3)
	if err := j.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("join walk failed: %v", err)
	}
}
