package sqlparser

import "testing"

func TestSelectStub_AddlCoverage(t *testing.T) {
	// Cover isEmpty branches.
	var nilWin *WindowExpr
	if !nilWin.isEmpty() {
		t.Fatalf("nil window should be empty")
	}
	if (&WindowExpr{}).isEmpty() != true {
		t.Fatalf("zero window should be empty")
	}
	if (&WindowExpr{Interval: Literal{Val: Token{Bytes: []byte("1s")}, Type: LiteralDuration}}).isEmpty() {
		t.Fatalf("window with interval should not be empty")
	}

	// Cover OrderBy nulls branches.
	tb := newTB()
	(&OrderByExpr{Expr: &RawExpr{Name: "a"}, Asc: true, NullsFirst: true}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("empty order by format")
	}
	tb = newTB()
	(&OrderByExpr{Expr: &RawExpr{Name: "b"}, Asc: false, NullsFirst: false}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("empty order by format")
	}

	// Cover SLimit branch and limit branch.
	tb = newTB()
	(&LimitExpr{SLimit: Token{Bytes: []byte("?")}, SOffset: Token{Bytes: []byte("?")}}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("empty slimit format")
	}
	tb = newTB()
	(&LimitExpr{Limit: Token{Bytes: []byte("10")}, Offset: Token{Bytes: []byte("5")}}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("empty limit format")
	}

	// Cover FillExpr format branches.
	tb = newTB()
	(&FillExpr{Name: "fill_name"}).Format(tb)
	tb = newTB()
	(&FillExpr{
		Mode: FILL_MODE_VALUE,
		Values: []Expr{
			Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt},
			Literal{Val: Token{Bytes: []byte("2")}, Type: LiteralInt},
		},
	}).Format(tb)
	if tb.String() == "" {
		t.Fatalf("empty fill format")
	}

	// Cover i* marker methods.
	(&TableNameExpr{}).iTableExpr()
	(&SubqueryTableExpr{}).iTableExpr()
	(&JoinTableExpr{}).iTableExpr()
	(&RawExpr{}).iExpr()
	(Literal{}).iExpr()
}

func TestJoinTableExpr_AllJoinTypes(t *testing.T) {
	joinTypes := []JoinType{
		JoinTypeInner,
		JoinTypeLeft,
		JoinTypeRight,
		JoinTypeFull,
		JoinTypeLeftSemi,
		JoinTypeRightSemi,
		JoinTypeLeftAnti,
		JoinTypeRightAnti,
		JoinTypeLeftAsof,
		JoinTypeRightAsof,
		JoinTypeLeftWindow,
		JoinTypeRightWindow,
	}
	for _, jt := range joinTypes {
		tb := newTB()
		j := &JoinTableExpr{
			Left:         &TableNameExpr{TableName: "t1"},
			Right:        &TableNameExpr{TableName: "t2"},
			JoinType:     jt,
			Condition:    &RawExpr{Name: "cond"},
			WindowOffset: &RawExpr{Name: "off"},
			JLimit:       &LimitExpr{Limit: Token{Bytes: []byte("1")}},
		}
		j.Format(tb)
		if tb.String() == "" {
			t.Fatalf("empty join format for %v", jt)
		}
	}
}

func TestRawExpr_AdditionalKindsAndOps(t *testing.T) {
	kinds := []Expr{
		&RawExpr{Kind: "pseudo_col", Name: "_wstart"},
		&RawExpr{Kind: "func", Name: "f", Args: []Expr{&RawExpr{Name: "a"}, &RawExpr{Name: "b"}}},
		&RawExpr{Kind: "unary", Op: Token{Bytes: []byte("uplus")}, Left: &RawExpr{Name: "a"}},
		&RawExpr{Kind: "unary", Op: Token{Bytes: []byte("minus")}, Left: &RawExpr{Name: "a"}},
		&RawExpr{Kind: "unary", Op: Token{Bytes: []byte("not")}, Left: &RawExpr{Name: "a"}},
		&RawExpr{Kind: "unary", Op: Token{Bytes: []byte("unknown")}, Left: &RawExpr{Name: "a"}},
		&RawExpr{Kind: "unary", Op: Token{Bytes: []byte("minus")}},
		&RawExpr{Kind: "binary", Name: "fallback"},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("custom_op2")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("add")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("sub")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("mul")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("div")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("rem")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("bit_and")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "binary", Op: Token{Bytes: []byte("bit_or")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("or")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("and")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("lt")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("gt")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("le")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("ge")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("ne")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("eq")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("like")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("not_like")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("match")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("nmatch")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("regexp")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("not_regexp")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "cmp", Op: Token{Bytes: []byte("contains")}, Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "json", Left: &RawExpr{Name: "a"}, Extra: Token{Bytes: []byte("x")}},
		&RawExpr{Kind: "json", Left: &RawExpr{Name: "a"}, Extra: "x"},
		&RawExpr{Kind: "json", Left: &RawExpr{Name: "a"}, Extra: 1},
		&RawExpr{Kind: "cast", Left: &RawExpr{Name: "a"}, Type: "varchar"},
		&RawExpr{Kind: "cast", Left: &RawExpr{Name: "a"}, Type: "int"},
		&RawExpr{Kind: "trim", Left: &RawExpr{Name: "a"}, Extra: "leading"},
		&RawExpr{Kind: "trim", Left: &RawExpr{Name: "a"}},
		&RawExpr{Kind: "trim"},
		&RawExpr{Kind: "trim_ext", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}, Extra: "both"},
		&RawExpr{Kind: "position", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "if", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}, Extra: &RawExpr{Name: "c"}},
		&RawExpr{Kind: "if", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}, Extra: &LimitExpr{Limit: Token{Bytes: []byte("1")}}},
		&RawExpr{Kind: "ifnull", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "nullif", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "coalesce", Args: []Expr{&RawExpr{Name: "a"}, &RawExpr{Name: "b"}}},
		&RawExpr{
			Kind: "case_when",
			Left: &RawExpr{Name: "base"},
			Extra: &caseWhenExtra{
				WhenThen: []WhenThenExpr{
					{When: &RawExpr{Name: "w1"}, Then: &RawExpr{Name: "t1"}},
					{When: &RawExpr{Name: "w2"}, Then: &RawExpr{Name: "t2"}},
				},
				ElseExpr: &RawExpr{Name: "e"},
			},
		},
		&RawExpr{Kind: "between", Left: &RawExpr{Name: "a"}, Extra: betweenExtra{From: &RawExpr{Name: "b"}, To: &RawExpr{Name: "c"}, Not: true}},
		&RawExpr{Kind: "is_null", Left: &RawExpr{Name: "a"}, Extra: true},
		&RawExpr{Kind: "in", Left: &RawExpr{Name: "a"}, Op: Token{Bytes: []byte("in")}, Args: []Expr{&RawExpr{Name: "b"}}},
		&RawExpr{Kind: "in", Left: &RawExpr{Name: "a"}, Op: Token{Bytes: []byte("not_in")}, Args: []Expr{&RawExpr{Name: "b"}}},
		&RawExpr{Kind: "in_subquery", Left: &RawExpr{Name: "a"}, Op: Token{Bytes: []byte("in")}, Extra: &SelectStmt{Select: []Expr{&RawExpr{Name: "x"}}}},
		&RawExpr{Kind: "window_offset", Left: &RawExpr{Name: "a"}, Right: &RawExpr{Name: "b"}},
		&RawExpr{Kind: "range_1", Args: []Expr{&RawExpr{Name: "a"}}},
		&RawExpr{Kind: "range_2", Args: []Expr{&RawExpr{Name: "a"}, &RawExpr{Name: "b"}}},
		&RawExpr{Kind: "range_3", Args: []Expr{&RawExpr{Name: "a"}, &RawExpr{Name: "b"}, &RawExpr{Name: "c"}}},
		&RawExpr{Kind: "partition_by", Args: []Expr{&RawExpr{Name: "a"}, &RawExpr{Name: "b"}}},
		&RawExpr{Kind: "unknown", Op: Token{Bytes: []byte("custom_op")}},
		&RawExpr{Kind: "unknown_kind"},
	}

	for i, expr := range kinds {
		tb := newTB()
		expr.Format(tb)
		if tb.String() == "" {
			t.Fatalf("empty raw expr format at %d", i)
		}
	}
}

func TestWindowExpr_AdditionalBranches(t *testing.T) {
	cases := []*WindowExpr{
		{
			StateWindow:    &RawExpr{Name: "v"},
			StateWindowOpt: StateWindowOpt{HasExtend: true, Extend: Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}, HasZeroth: true, Zeroth: Literal{Val: Token{Bytes: []byte("z")}, Type: LiteralString}},
			TrueFor:        Literal{Val: Token{Bytes: []byte("1s")}, Type: LiteralDuration},
		},
		{
			Session:    &RawExpr{Name: "c1"},
			SessionGap: Literal{Val: Token{Bytes: []byte("10s")}, Type: LiteralDuration},
		},
		{
			Interval: Literal{Val: Token{Bytes: []byte("10s")}, Type: LiteralDuration},
			Offset:   Literal{Val: Token{Bytes: []byte("1s")}, Type: LiteralDuration},
			Sliding:  Literal{Val: Token{Bytes: []byte("2s")}, Type: LiteralDuration},
			Fill:     &FillExpr{Mode: FILL_MODE_PREV},
		},
		{
			EventWindowStart: &RawExpr{Name: "a"},
			EventWindowEnd:   &RawExpr{Name: "b"},
			TrueFor:          Literal{Val: Token{Bytes: []byte("5s")}, Type: LiteralDuration},
		},
		{
			CountWindow:      Token{Bytes: []byte("10")},
			CountWindowSlide: Token{Bytes: []byte("2")},
			CountWindowCols:  []ColumnExpr{"c1", "c2"},
		},
		{
			AnomalyWindow: &RawExpr{Name: "v"},
			AnomalyTag:    Token{Bytes: []byte("strict")},
		},
	}
	for i, c := range cases {
		tb := newTB()
		c.Format(tb)
		if tb.String() == "" {
			t.Fatalf("empty window format at %d", i)
		}
	}
}

func TestHintTypeToSQL_AllBranches(t *testing.T) {
	for _, h := range []HintType{
		HINT_NO_BATCH_SCAN,
		HINT_BATCH_SCAN,
		HINT_SORT_FOR_GROUP,
		HINT_PARTITION_FIRST,
		HINT_PARA_TABLES_SORT,
		HINT_SMALLDATA_TS_SORT,
		HINT_HASH_JOIN,
		HINT_SKIP_TSMA,
		HINT_WIN_OPTIMIZE_BATCH,
		HINT_WIN_OPTIMIZE_SINGLE,
		HintType(0),
	} {
		_ = hintTypeToSQL(h)
	}
}
