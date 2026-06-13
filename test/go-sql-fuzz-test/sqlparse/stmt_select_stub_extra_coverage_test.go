package sqlparser

import (
	"errors"
	"testing"
)

func TestSelectStub_NodeFormatsAndWalks(t *testing.T) {
	tb := newTB()

	var nilOrder *OrderByExpr
	nilOrder.Format(tb)
	if err := nilOrder.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil order walk failed: %v", err)
	}

	tb.Reset()
	order := &OrderByExpr{Expr: &RawExpr{Name: "c1"}, Asc: false}
	order.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected order format output")
	}

	var nilGroup *GroupByExpr
	nilGroup.Format(tb)
	if err := nilGroup.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil group walk failed: %v", err)
	}
	group := &GroupByExpr{Exprs: []Expr{&RawExpr{Name: "c1"}, &RawExpr{Name: "c2"}}}
	tb.Reset()
	group.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected group format output")
	}
	if err := group.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("group walk failed: %v", err)
	}

	var nilLimit *LimitExpr
	nilLimit.Format(tb)
	if err := nilLimit.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil limit walk failed: %v", err)
	}
	limit := &LimitExpr{Limit: Token{Bytes: []byte("10")}, Offset: Token{Bytes: []byte("5")}}
	tb.Reset()
	limit.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected limit format output")
	}

	var nilWindow *WindowExpr
	nilWindow.Format(tb)
	if err := nilWindow.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil window walk failed: %v", err)
	}
	window := &WindowExpr{
		Interval:         Literal{Val: Token{Bytes: []byte("10s")}, Type: LiteralDuration},
		Session:          &RawExpr{Name: "session_col"},
		StateWindow:      &RawExpr{Name: "state_col"},
		EventWindowStart: &RawExpr{Name: "ws"},
		EventWindowEnd:   &RawExpr{Name: "we"},
		AnomalyWindow:    &RawExpr{Name: "aw"},
		Fill:             &FillExpr{Name: "linear", Values: []Expr{&RawExpr{Name: "fv"}}},
		CountWindowCols:  []ColumnExpr{"c1"},
	}
	tb.Reset()
	window.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected window format output")
	}
	if err := window.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("window walk failed: %v", err)
	}

	var nilFill *FillExpr
	nilFill.Format(tb)
	if err := nilFill.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil fill walk failed: %v", err)
	}
	fill := &FillExpr{Name: "value", Mode: FILL_MODE_NEAR, Values: []Expr{&RawExpr{Name: "c1"}}}
	tb.Reset()
	fill.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected fill format output")
	}
	if err := fill.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("fill walk failed: %v", err)
	}

	var nilTable *TableNameExpr
	nilTable.Format(tb)
	if err := nilTable.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil tablename walk failed: %v", err)
	}
	table := &TableNameExpr{DBName: "db1", TableName: "t1", Alias: "a"}
	tb.Reset()
	table.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected tablename format output")
	}

	var nilSub *SubqueryTableExpr
	nilSub.Format(tb)
	if err := nilSub.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil subquery walk failed: %v", err)
	}
	sub := &SubqueryTableExpr{Query: &SelectStmt{}, Alias: "sq"}
	tb.Reset()
	sub.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected subquery format output")
	}
	if err := sub.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("subquery walk failed: %v", err)
	}

	var nilJoin *JoinTableExpr
	nilJoin.Format(tb)
	if err := nilJoin.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil join walk failed: %v", err)
	}
	join := &JoinTableExpr{
		Left:         table,
		Right:        sub,
		Condition:    &RawExpr{Name: "jc"},
		WindowOffset: &RawExpr{Name: "wo"},
	}
	tb.Reset()
	join.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected join format output")
	}
	if err := join.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("join walk failed: %v", err)
	}

	var nilRaw *RawExpr
	if nilRaw.replace(&RawExpr{Name: "x"}, &RawExpr{Name: "y"}) {
		t.Fatalf("nil raw replace should be false")
	}
	nilRaw.Format(tb)
	if err := nilRaw.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil raw walk failed: %v", err)
	}
	l := &RawExpr{Name: "l"}
	r := &RawExpr{Name: "r"}
	raw := &RawExpr{
		Kind:  "binary",
		Left:  l,
		Right: r,
		Args:  []Expr{&RawExpr{Name: "a1"}, &RawExpr{Name: "a2"}},
		Extra: &RawExpr{Name: "extra"},
	}
	if !raw.replace(l, &RawExpr{Name: "l2"}) {
		t.Fatalf("expected replace on left")
	}
	if !raw.replace(r, &RawExpr{Name: "r2"}) {
		t.Fatalf("expected replace on right")
	}
	if !raw.replace(raw.Args[0], &RawExpr{Name: "a3"}) {
		t.Fatalf("expected replace on args")
	}
	tb.Reset()
	raw.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected raw format output")
	}
	if err := raw.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("raw walk failed: %v", err)
	}

	tb.Reset()
	(&RawExpr{Op: Token{Bytes: []byte("op")}}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected raw op format output")
	}
	tb.Reset()
	(Literal{Val: Token{Bytes: []byte("10")}, Type: LiteralInt}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected literal format output")
	}
}

func TestSelectStub_HelperConstructorsAndBranches(t *testing.T) {
	if got := NewHintOptionFromHintToken(Token{Bytes: []byte("batch_scan()")}); got == nil || got.HintType != HINT_BATCH_SCAN {
		t.Fatalf("unexpected direct hint parse: %+v", got)
	}
	if got := NewHintOptionFromHintToken(Token{Bytes: []byte("unknown(), hash_join()")}); got == nil || got.HintType != HINT_HASH_JOIN {
		t.Fatalf("unexpected csv hint parse: %+v", got)
	}
	if got := NewHintOptionFromHintToken(Token{Bytes: []byte("unknown()")}); got != nil {
		t.Fatalf("expected nil hint for unknown token")
	}
	if got := hintOptionFromHintName("win_optimize_single()"); got == nil || got.HintType != HINT_WIN_OPTIMIZE_SINGLE {
		t.Fatalf("unexpected hint option mapping: %+v", got)
	}
	if got := hintOptionFromHintName("not_exists()"); got != nil {
		t.Fatalf("expected nil for unknown hint")
	}

	base := NewSelectStmt(nil, NewHintOption(HINT_BATCH_SCAN), true, true, []Expr{&RawExpr{Name: "c"}}, &TableNameExpr{TableName: "t"}, &RawExpr{Name: "w"}, nil, nil, Literal{}, nil, WindowExpr{}, nil, nil)
	if !base.IsDistinct || !base.TagScan {
		t.Fatalf("new select stmt fields not set")
	}
	withClauses := NewSelectStmtWithClauses(nil, nil, []OrderByExpr{{Expr: &RawExpr{Name: "o"}}}, &LimitExpr{SLimit: Token{Bytes: []byte("1")}}, &LimitExpr{Limit: Token{Bytes: []byte("10")}})
	if withClauses == nil || len(withClauses.OrderBy) != 1 {
		t.Fatalf("expected select with clauses")
	}
	withClauses = NewSelectStmtWithClauses(nil, withClauses, nil, nil, nil)
	if withClauses == nil {
		t.Fatalf("expected non-nil select on reuse")
	}
	if u := NewUnionStmt(nil, &SelectStmt{}, &SelectStmt{}, true); u.SetOp != "union" || !u.SetAll {
		t.Fatalf("unexpected union stmt: %+v", u)
	}
	if e := NewExceptStmt(nil, &SelectStmt{}, &SelectStmt{}); e.SetOp != "except" {
		t.Fatalf("unexpected except stmt: %+v", e)
	}
	if i := NewIntersectStmt(nil, &SelectStmt{}, &SelectStmt{}); i.SetOp != "intersect" {
		t.Fatalf("unexpected intersect stmt: %+v", i)
	}

	tbl := NewTableNameExpr(nil, "db", "tbl", "a")
	sub := NewSubqueryTableExpr(nil, &SelectStmt{}, "s")
	join := NewJoinTableExpr(nil, tbl, sub, JoinTypeInner, &RawExpr{Name: "cond"})
	if SetJoinWindowOffsetAndLimit(nil, nil, nil) != nil {
		t.Fatalf("nil join should stay nil")
	}
	join = SetJoinWindowOffsetAndLimit(join, &RawExpr{Name: "wo"}, &LimitExpr{Limit: Token{Bytes: []byte("1")}})
	if join == nil || join.WindowOffset == nil || join.JLimit == nil {
		t.Fatalf("expected join window settings")
	}

	if _, ok := NewWindowOffsetExpr(&RawExpr{Name: "s"}, &RawExpr{Name: "e"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewWindowOffsetExpr")
	}
	if _, ok := NewUnaryExpr(nil, OP_TYPE_MINUS, &RawExpr{Name: "x"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewUnaryExpr")
	}
	if _, ok := NewBinaryExpr(nil, &RawExpr{Name: "l"}, OP_TYPE_ADD, &RawExpr{Name: "r"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewBinaryExpr")
	}
	if _, ok := NewJsonExpr(nil, &RawExpr{Name: "l"}, Token{Bytes: []byte("$.a")}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewJsonExpr")
	}
	if _, ok := NewColNameExpr(nil, "t", "c").(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewColNameExpr")
	}
	if _, ok := NewPseudoColumnExpr(nil, "_rowts").(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewPseudoColumnExpr")
	}
	if _, ok := NewFuncExpr(nil, "sum", []Expr{&RawExpr{Name: "x"}}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewFuncExpr")
	}
	if _, ok := NewCastExpr(nil, &RawExpr{Name: "x"}, "int").(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewCastExpr")
	}
	if _, ok := NewTrimExpr(nil, &RawExpr{Name: "x"}, "leading").(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewTrimExpr")
	}
	if _, ok := NewTrimExprWithPattern(nil, &RawExpr{Name: "x"}, &RawExpr{Name: "y"}, "both").(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewTrimExprWithPattern")
	}
	if _, ok := NewPositionExpr(nil, &RawExpr{Name: "x"}, &RawExpr{Name: "y"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewPositionExpr")
	}
	if _, ok := NewIfExpr(nil, &RawExpr{Name: "c"}, &RawExpr{Name: "t"}, &RawExpr{Name: "f"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewIfExpr")
	}
	if _, ok := NewIfNullExpr(nil, &RawExpr{Name: "x"}, &RawExpr{Name: "y"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewIfNullExpr")
	}
	if _, ok := NewNullIfExpr(nil, &RawExpr{Name: "x"}, &RawExpr{Name: "y"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewNullIfExpr")
	}
	if _, ok := NewCoalesceExpr(nil, []Expr{&RawExpr{Name: "x"}}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewCoalesceExpr")
	}
	if _, ok := NewCaseWhenExpr(nil, &RawExpr{Name: "b"}, []WhenThenExpr{{When: &RawExpr{Name: "w"}, Then: &RawExpr{Name: "t"}}}, &RawExpr{Name: "e"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewCaseWhenExpr")
	}
	if lit := NewLiteralExpr(nil, Token{Bytes: []byte("1")}, LiteralInt); lit.Type != LiteralInt {
		t.Fatalf("unexpected literal type: %+v", lit)
	}
	if _, ok := NewComparisonExpr(nil, &RawExpr{Name: "l"}, OP_TYPE_EQUAL, &RawExpr{Name: "r"}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewComparisonExpr")
	}
	if _, ok := NewBetweenExpr(nil, &RawExpr{Name: "x"}, &RawExpr{Name: "1"}, &RawExpr{Name: "2"}, false).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewBetweenExpr")
	}
	if _, ok := NewIsNullExpr(nil, &RawExpr{Name: "x"}, true).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewIsNullExpr")
	}
	if _, ok := NewInExpr(nil, &RawExpr{Name: "x"}, OP_TYPE_IN, []Expr{Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewInExpr")
	}
	if _, ok := NewInSubqueryExpr(nil, &RawExpr{Name: "x"}, OP_TYPE_IN, &SelectStmt{}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewInSubqueryExpr")
	}
	if _, ok := NewPartitionByExpr(nil, []Expr{&RawExpr{Name: "x"}}).(*RawExpr); !ok {
		t.Fatalf("expected raw expr from NewPartitionByExpr")
	}

	inList := &RawExpr{Kind: "in_list", Args: []Expr{Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}}}
	if got := NewInPredicateExpr(nil, &RawExpr{Name: "x"}, OP_TYPE_IN, inList).(*RawExpr); got.Kind != "in" || len(got.Args) != 1 {
		t.Fatalf("unexpected in-list predicate: %+v", got)
	}
	if got := NewInPredicateExpr(nil, &RawExpr{Name: "x"}, OP_TYPE_IN, &SelectStmt{}).(*RawExpr); got.Kind != "in_subquery" {
		t.Fatalf("unexpected in-subquery predicate: %+v", got)
	}
	if got := NewInPredicateExpr(nil, &RawExpr{Name: "x"}, OP_TYPE_IN, &RawExpr{Kind: "other"}).(*RawExpr); got.Kind != "in" {
		t.Fatalf("unexpected fallback in predicate: %+v", got)
	}

	win := NewIntervalAutoWindowExpr(nil, Literal{Val: Token{Bytes: []byte("1m")}, Type: LiteralDuration}, Token{Bytes: []byte("auto")}, Literal{Val: Token{Bytes: []byte("2m")}, Type: LiteralDuration}, FILL_MODE_LINEAR)
	if string(win.Offset.Val.Bytes) != "auto" {
		t.Fatalf("unexpected interval auto offset: %+v", win)
	}
}

func TestSelectStmt_WalkErrorBranches(t *testing.T) {
	wantErr := errors.New("walk-stop")

	withSelect := &SelectStmt{Select: []Expr{&RawExpr{Name: "s"}}}
	if err := withSelect.walkSubtree(func(node SQLNode) (bool, error) {
		if _, ok := node.(*RawExpr); ok {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected select loop error, got: %v", err)
	}

	withMain := &SelectStmt{From: &TableNameExpr{TableName: "t"}}
	if err := withMain.walkSubtree(func(node SQLNode) (bool, error) {
		if _, ok := node.(*TableNameExpr); ok {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected main walk error, got: %v", err)
	}

	withOrder := &SelectStmt{OrderBy: []OrderByExpr{{Expr: &RawExpr{Name: "o"}}}}
	if err := withOrder.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "o" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected order walk error, got: %v", err)
	}
}

func TestSelectStub_RemainingErrorAndTrueBranches(t *testing.T) {
	wantErr := errors.New("branch-error")

	asc := &OrderByExpr{Expr: &RawExpr{Name: "x"}, Asc: true}
	tb := newTB()
	asc.Format(tb)
	if tb.String() == "" {
		t.Fatalf("expected asc format output")
	}
	if err := asc.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("order walk should succeed: %v", err)
	}

	group := &GroupByExpr{Exprs: []Expr{&RawExpr{Name: "g1"}}}
	if err := group.walkSubtree(func(node SQLNode) (bool, error) {
		if _, ok := node.(*RawExpr); ok {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected group walk error, got: %v", err)
	}

	window := &WindowExpr{Session: &RawExpr{Name: "sess"}}
	if err := window.walkSubtree(func(node SQLNode) (bool, error) {
		if _, ok := node.(*RawExpr); ok {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected window walk error, got: %v", err)
	}

	fillModeErr := &FillExpr{Name: "v", Mode: &FillExpr{Name: "mode"}}
	if err := fillModeErr.walkSubtree(func(node SQLNode) (bool, error) {
		if f, ok := node.(*FillExpr); ok && f.Name == "mode" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected fill mode walk error, got: %v", err)
	}

	fillValueErr := &FillExpr{Name: "v", Values: []Expr{&RawExpr{Name: "fv"}}}
	if err := fillValueErr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "fv" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected fill value walk error, got: %v", err)
	}

	joinWalkErr := &JoinTableExpr{
		Left:         &TableNameExpr{TableName: "l"},
		Right:        &TableNameExpr{TableName: "r"},
		WindowOffset: &RawExpr{Name: "wo"},
		Condition:    &RawExpr{Name: "cond"},
	}
	if err := joinWalkErr.walkSubtree(func(node SQLNode) (bool, error) {
		if t, ok := node.(*TableNameExpr); ok && t.TableName == "l" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected join walk pre-condition error, got: %v", err)
	}
	if err := joinWalkErr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "cond" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected join condition walk error, got: %v", err)
	}

	target := &RawExpr{Name: "target"}
	to := &RawExpr{Name: "to"}
	rawLeftRecursive := &RawExpr{Left: &RawExpr{Left: target}}
	if !rawLeftRecursive.replace(target, to) {
		t.Fatalf("expected recursive left replace")
	}
	rawRightRecursive := &RawExpr{Right: &RawExpr{Right: target}}
	if !rawRightRecursive.replace(target, to) {
		t.Fatalf("expected recursive right replace")
	}
	rawArgRecursive := &RawExpr{Args: []Expr{&RawExpr{Left: target}}}
	if !rawArgRecursive.replace(target, to) {
		t.Fatalf("expected recursive args replace")
	}

	rawWalkErrLR := &RawExpr{Left: &RawExpr{Name: "l"}}
	if err := rawWalkErrLR.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "l" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected raw left/right walk error, got: %v", err)
	}
	rawWalkErrArg := &RawExpr{Args: []Expr{&RawExpr{Name: "a"}}}
	if err := rawWalkErrArg.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "a" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected raw arg walk error, got: %v", err)
	}
	rawWalkExtraSelect := &RawExpr{Extra: &SelectStmt{Select: []Expr{&RawExpr{Name: "sx"}}}}
	if err := rawWalkExtraSelect.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("expected raw extra-select walk success, got: %v", err)
	}

	caseExpr := NewCaseWhenExpr(nil,
		&RawExpr{Name: "base"},
		[]WhenThenExpr{
			{When: &RawExpr{Name: "when1"}, Then: &RawExpr{Name: "then1"}},
			{When: &RawExpr{Name: "when2"}, Then: &RawExpr{Name: "then2"}},
		},
		&RawExpr{Name: "else1"},
	)
	if err := caseExpr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "then2" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected case-when walk error, got: %v", err)
	}
	caseExprOK := NewCaseWhenExpr(nil,
		&RawExpr{Name: "base2"},
		[]WhenThenExpr{
			{When: &RawExpr{Name: "when3"}, Then: &RawExpr{Name: "then3"}},
		},
		&RawExpr{Name: "else2"},
	)
	visitedElse := false
	if err := caseExprOK.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "else2" {
			visitedElse = true
		}
		return true, nil
	}); err != nil {
		t.Fatalf("expected case-when walk success, got: %v", err)
	}
	if !visitedElse {
		t.Fatalf("expected case-when walk to visit else expression")
	}
	rawCasePtr := &RawExpr{
		Extra: &caseWhenExtra{
			WhenThen: []WhenThenExpr{
				{When: &RawExpr{Name: "pwhen"}, Then: &RawExpr{Name: "pthen"}},
			},
			ElseExpr: &RawExpr{Name: "pelse"},
		},
	}
	if err := rawCasePtr.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("expected raw case pointer walk success, got: %v", err)
	}
	if err := rawCasePtr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "pthen" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected raw case pointer walk error, got: %v", err)
	}
	rawCasePtrNil := &RawExpr{Extra: (*caseWhenExtra)(nil)}
	if err := rawCasePtrNil.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("expected raw nil case pointer walk success, got: %v", err)
	}

	betweenExpr := NewBetweenExpr(nil, &RawExpr{Name: "target"}, &RawExpr{Name: "from1"}, &RawExpr{Name: "to1"}, false)
	if err := betweenExpr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "to1" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected between walk error, got: %v", err)
	}
	rawBetweenPtr := &RawExpr{Extra: &betweenExtra{From: &RawExpr{Name: "pf"}, To: &RawExpr{Name: "pt"}}}
	if err := rawBetweenPtr.walkSubtree(func(node SQLNode) (bool, error) {
		if r, ok := node.(*RawExpr); ok && r.Name == "pt" {
			return false, wantErr
		}
		return true, nil
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected between pointer walk error, got: %v", err)
	}
	rawBetweenPtrNil := &RawExpr{Extra: (*betweenExtra)(nil)}
	if err := rawBetweenPtrNil.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("expected raw nil between pointer walk success, got: %v", err)
	}
}
