package sqlparser

import (
	"strings"
	"testing"
)

func litExpr(v string) Literal {
	return Literal{Val: Token{Bytes: []byte(v)}, Type: LiteralInt}
}

func TestSelectStmt_BasicMethodsAndSetters(t *testing.T) {
	s := &SelectStmt{}
	s.iStatement()
	s.iExpr()
	if s.replace(&RawExpr{Kind: "col", Name: "a"}, &RawExpr{Kind: "col", Name: "b"}) {
		t.Fatalf("replace should be false in stub")
	}
	tb := newTB()
	s.Format(tb)
	if tb.String() != "select *" {
		t.Fatalf("unexpected select format output: %q", tb.String())
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("walkSubtree failed: %v", err)
	}

	s.SetSelectStmtTagMode(true)
	if !s.TagScan {
		t.Fatalf("tag mode setter failed")
	}
}

func TestSelectStmt_FormatCoverageBranches(t *testing.T) {
	// Set-op branch with ALL.
	left := &SelectStmt{Select: []Expr{&RawExpr{Name: "a"}}}
	right := &SelectStmt{Select: []Expr{&RawExpr{Name: "b"}}}
	setOp := &SelectStmt{Left: left, Right: right, SetOp: "union", SetAll: true}
	tbSetOp := newTB()
	setOp.Format(tbSetOp)
	if got := tbSetOp.String(); got == "" {
		t.Fatalf("empty set-op format")
	}

	// Non-empty select list, where/group/having/order/limit branches.
	s := &SelectStmt{
		Select: []Expr{&RawExpr{Name: "c1"}, &RawExpr{Name: "c2"}},
		From:   &TableNameExpr{TableName: "t1"},
		Where:  &RawExpr{Name: "w"},
		GroupBy: &GroupByExpr{
			Exprs: []Expr{&RawExpr{Name: "g1"}},
		},
		Having: &RawExpr{Name: "h1"},
		OrderBy: []OrderByExpr{
			{Expr: &RawExpr{Name: "o1"}, Asc: true},
			{Expr: &RawExpr{Name: "o2"}, Asc: false},
		},
		Limit:      &LimitExpr{Limit: Token{Bytes: []byte("10")}},
		InterpFill: &FillExpr{Mode: FILL_MODE_PREV},
	}
	tb := newTB()
	s.Format(tb)
	out := tb.String()
	if out == "" {
		t.Fatalf("empty select format")
	}
	if out == "select *" {
		t.Fatalf("expected non-trivial select format, got %q", out)
	}
	if out != "" && !strings.Contains(out, "fill(") {
		t.Fatalf("expected fill clause in formatted select, got %q", out)
	}
}

func TestSelectStmt_NilReceiverBranches(t *testing.T) {
	var s *SelectStmt
	tb := newTB()
	s.Format(tb)
	if tb.String() != "" {
		t.Fatalf("nil select format should be empty, got %q", tb.String())
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil select walk should be nil error, got %v", err)
	}
}

func TestStmtSelectStub_ConstructorCoverage(t *testing.T) {
	left := NewTableNameExpr(nil, "db1", "t1", "a")
	right := NewSubqueryTableExpr(nil, &SelectStmt{SetOp: "union"}, "s")
	join := NewJoinTableExpr(nil, left, right, JoinTypeLeft, "on")
	if join.JoinType != JoinTypeLeft {
		t.Fatalf("join type not set")
	}
	if SetJoinWindowOffsetAndLimit(nil, nil, nil) != nil {
		t.Fatalf("nil join should keep nil")
	}
	offset := NewWindowOffsetExpr(&RawExpr{Kind: "col", Name: "s"}, &RawExpr{Kind: "col", Name: "e"})
	lim := &LimitExpr{Limit: Token{Bytes: []byte("10")}}
	join = SetJoinWindowOffsetAndLimit(join, offset, lim)
	if join.WindowOffset == nil || join.JLimit == nil {
		t.Fatalf("join window offset/limit not set")
	}

	base := NewSelectStmt(nil, NewHintOption(HINT_HASH_JOIN), true, true, []Expr{litExpr("1")}, left, nil, nil, nil, Literal{}, nil, WindowExpr{}, nil, nil)
	withClauses := NewSelectStmtWithClauses(nil, nil, []OrderByExpr{{Expr: litExpr("1"), Asc: true}}, nil, lim)
	if withClauses == nil || withClauses.Limit == nil {
		t.Fatalf("nil-base select clauses builder failed")
	}
	_ = NewSelectStmtWithClauses(nil, base, nil, nil, nil)

	_ = NewUnionStmt(nil, &SelectStmt{}, &SelectStmt{}, true)
	if ex := NewExceptStmt(nil, &SelectStmt{}, &SelectStmt{}); ex.SetOp != "except" {
		t.Fatalf("except set-op mismatch")
	}
	if in := NewIntersectStmt(nil, &SelectStmt{}, &SelectStmt{}); in.SetOp != "intersect" {
		t.Fatalf("intersect set-op mismatch")
	}

	_ = NewUnaryExpr(nil, OP_TYPE_UPLUS, litExpr("1"))
	_ = NewBinaryExpr(nil, litExpr("1"), OP_TYPE_ADD, litExpr("2"))
	_ = NewJsonExpr(nil, &RawExpr{Kind: "col", Name: "j"}, Token{Bytes: []byte("$.a")})
	_ = NewColNameExpr(nil, "t", "c")
	_ = NewPseudoColumnExpr(nil, "_wstart")
	_ = NewFuncExpr(nil, "f", []Expr{litExpr("1")})
	_ = NewCastExpr(nil, litExpr("1"), "int")
	_ = NewTrimExpr(nil, litExpr("1"), "leading")
	_ = NewTrimExprWithPattern(nil, litExpr("1"), litExpr("2"), "both")
	_ = NewPositionExpr(nil, litExpr("1"), litExpr("2"))
	_ = NewIfExpr(nil, litExpr("1"), litExpr("2"), litExpr("3"))
	_ = NewIfNullExpr(nil, litExpr("1"), litExpr("2"))
	_ = NewNullIfExpr(nil, litExpr("1"), litExpr("2"))
	_ = NewCoalesceExpr(nil, []Expr{litExpr("1"), litExpr("2")})
	_ = NewCaseWhenExpr(nil, litExpr("1"), []WhenThenExpr{{When: litExpr("2"), Then: litExpr("3")}}, litExpr("4"))
	_ = NewComparisonExpr(nil, litExpr("1"), OP_TYPE_EQUAL, litExpr("2"))
	_ = NewBetweenExpr(nil, litExpr("5"), litExpr("1"), litExpr("10"), true)
	_ = NewIsNullExpr(nil, litExpr("1"), false)
	_ = NewInExpr(nil, litExpr("1"), OP_TYPE_IN, []Expr{litExpr("2")})
	_ = NewInSubqueryExpr(nil, litExpr("1"), OP_TYPE_NOT_IN, &SelectStmt{})
	_ = NewPartitionByExpr(nil, []Expr{litExpr("1")})

	lit := NewLiteralExpr(nil, Token{Bytes: []byte("x")}, LiteralString)
	tb := newTB()
	lit.Format(tb)
	if lit.replace(litExpr("1"), litExpr("2")) {
		t.Fatalf("literal replace should be false")
	}
	if err := lit.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("literal walk failed: %v", err)
	}

	raw := &RawExpr{Kind: "x"}
	if raw.replace(litExpr("1"), litExpr("2")) {
		t.Fatalf("raw replace should be false")
	}
	raw.Format(newTB())
	if err := raw.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("raw walk failed: %v", err)
	}
}

func TestStmtSelectStub_HintParsingFallback(t *testing.T) {
	if h := NewHintOptionFromHintToken(Token{Bytes: []byte("unknown_hint()")}); h != nil {
		t.Fatalf("unexpected hint for unknown token: %+v", h)
	}
	if h := NewHintOptionFromHintToken(Token{Bytes: []byte("unknown(), hash_join()")}); h == nil || h.HintType != HINT_HASH_JOIN {
		t.Fatalf("expected hash_join fallback, got %+v", h)
	}
}
