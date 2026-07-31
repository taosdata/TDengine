package sqlparser

import "testing"

func TestSelectStmt_ReplaceBranches(t *testing.T) {
	src := &RawExpr{Name: "src"}
	dst := &RawExpr{Name: "dst"}

	s := &SelectStmt{
		Select:    []Expr{src},
		Where:     &RawExpr{Name: "w"},
		Partition: &RawExpr{Name: "p"},
		Range:     &RawExpr{Name: "r"},
		Having:    &RawExpr{Name: "h"},
		GroupBy:   &GroupByExpr{Exprs: []Expr{&RawExpr{Name: "g"}}},
		OrderBy:   []OrderByExpr{{Expr: &RawExpr{Name: "o"}}},
		Window: WindowExpr{
			Session:          &RawExpr{Name: "ws"},
			StateWindow:      &RawExpr{Name: "stw"},
			EventWindowStart: &RawExpr{Name: "ews"},
			EventWindowEnd:   &RawExpr{Name: "ewe"},
			AnomalyWindow:    &RawExpr{Name: "aw"},
		},
		InterpFill: &FillExpr{
			Name:   "value",
			Values: []Expr{&RawExpr{Name: "fv"}},
		},
		Left:  &SelectStmt{Where: &RawExpr{Name: "lw"}},
		Right: &SelectStmt{Having: src},
	}

	if !s.replace(src, dst) {
		t.Fatalf("expected replace in select list")
	}
	if s.Select[0] != dst {
		t.Fatalf("select list replace not applied")
	}

	targets := []struct {
		name string
		set  func() Expr
		get  func() Expr
	}{
		{
			name: "where",
			set:  func() Expr { s.Where = src; return s.Where },
			get:  func() Expr { return s.Where },
		},
		{
			name: "partition",
			set:  func() Expr { s.Partition = src; return s.Partition },
			get:  func() Expr { return s.Partition },
		},
		{
			name: "range",
			set:  func() Expr { s.Range = src; return s.Range },
			get:  func() Expr { return s.Range },
		},
		{
			name: "having",
			set:  func() Expr { s.Having = src; return s.Having },
			get:  func() Expr { return s.Having },
		},
		{
			name: "group by",
			set:  func() Expr { s.GroupBy.Exprs[0] = src; return s.GroupBy.Exprs[0] },
			get:  func() Expr { return s.GroupBy.Exprs[0] },
		},
		{
			name: "order by",
			set:  func() Expr { s.OrderBy[0].Expr = src; return s.OrderBy[0].Expr },
			get:  func() Expr { return s.OrderBy[0].Expr },
		},
		{
			name: "window session",
			set:  func() Expr { s.Window.Session = src; return s.Window.Session },
			get:  func() Expr { return s.Window.Session },
		},
		{
			name: "window state",
			set:  func() Expr { s.Window.StateWindow = src; return s.Window.StateWindow },
			get:  func() Expr { return s.Window.StateWindow },
		},
		{
			name: "window start",
			set:  func() Expr { s.Window.EventWindowStart = src; return s.Window.EventWindowStart },
			get:  func() Expr { return s.Window.EventWindowStart },
		},
		{
			name: "window end",
			set:  func() Expr { s.Window.EventWindowEnd = src; return s.Window.EventWindowEnd },
			get:  func() Expr { return s.Window.EventWindowEnd },
		},
		{
			name: "window anomaly",
			set:  func() Expr { s.Window.AnomalyWindow = src; return s.Window.AnomalyWindow },
			get:  func() Expr { return s.Window.AnomalyWindow },
		},
		{
			name: "interp fill value",
			set:  func() Expr { s.InterpFill.Values[0] = src; return s.InterpFill.Values[0] },
			get:  func() Expr { return s.InterpFill.Values[0] },
		},
		{
			name: "right subtree",
			set:  func() Expr { s.Right.Having = src; return s.Right.Having },
			get:  func() Expr { return s.Right.Having },
		},
	}

	for _, tc := range targets {
		tc.set()
		if !s.replace(src, dst) {
			t.Fatalf("%s: expected replace", tc.name)
		}
		if tc.get() != dst {
			t.Fatalf("%s: replace not applied", tc.name)
		}
	}

	var nilStmt *SelectStmt
	if nilStmt.replace(src, dst) {
		t.Fatalf("nil replace should be false")
	}
	if s.replace(&RawExpr{Name: "missing"}, dst) {
		t.Fatalf("replace should fail for missing source")
	}
}

func TestSelectStmt_ReplaceRecursiveBranches(t *testing.T) {
	src := &RawExpr{Name: "src"}
	dst := &RawExpr{Name: "dst"}

	cases := []struct {
		name string
		stmt *SelectStmt
	}{
		{name: "select nested", stmt: &SelectStmt{Select: []Expr{&RawExpr{Left: src}}}},
		{name: "where nested", stmt: &SelectStmt{Where: &RawExpr{Left: src}}},
		{name: "partition nested", stmt: &SelectStmt{Partition: &RawExpr{Left: src}}},
		{name: "range nested", stmt: &SelectStmt{Range: &RawExpr{Left: src}}},
		{name: "having nested", stmt: &SelectStmt{Having: &RawExpr{Left: src}}},
		{name: "group nested", stmt: &SelectStmt{GroupBy: &GroupByExpr{Exprs: []Expr{&RawExpr{Left: src}}}}},
		{name: "order nested", stmt: &SelectStmt{OrderBy: []OrderByExpr{{Expr: &RawExpr{Left: src}}}}},
		{name: "window session nested", stmt: &SelectStmt{Window: WindowExpr{Session: &RawExpr{Left: src}}}},
		{name: "window state nested", stmt: &SelectStmt{Window: WindowExpr{StateWindow: &RawExpr{Left: src}}}},
		{name: "window start nested", stmt: &SelectStmt{Window: WindowExpr{EventWindowStart: &RawExpr{Left: src}}}},
		{name: "window end nested", stmt: &SelectStmt{Window: WindowExpr{EventWindowEnd: &RawExpr{Left: src}}}},
		{name: "window anomaly nested", stmt: &SelectStmt{Window: WindowExpr{AnomalyWindow: &RawExpr{Left: src}}}},
		{name: "interp fill nested", stmt: &SelectStmt{InterpFill: &FillExpr{Values: []Expr{&RawExpr{Left: src}}}}},
		{name: "left nested", stmt: &SelectStmt{Left: &SelectStmt{Where: &RawExpr{Left: src}}}},
		{name: "right nested", stmt: &SelectStmt{Right: &SelectStmt{Having: &RawExpr{Left: src}}}},
	}

	for _, tc := range cases {
		if !tc.stmt.replace(src, dst) {
			t.Fatalf("%s: expected recursive replace", tc.name)
		}
	}
}
