package sqlparser

import "testing"

func TestExprRuntimeContracts_ZeroAndComposite(t *testing.T) {
	exprs := []Expr{
		&AliasedExpr{},
		BoolVal(false),
		Literal{},
		&RawExpr{},
		&SQLVal{},
		&SelectStmt{},
		&StarExpr{},
	}

	for _, e := range exprs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic on expr %T: %v", e, r)
				}
			}()
			tb := newTB()
			e.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("replace panic on expr %T: %v", e, r)
				}
			}()
			_ = e.replace(&RawExpr{Name: "from"}, &RawExpr{Name: "to"})
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic on expr %T: %v", e, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, e); err != nil {
				t.Fatalf("Walk failed on expr %T: %v", e, err)
			}
		}()
	}

	complex := NewCaseWhenExpr(nil,
		&RawExpr{Name: "base"},
		[]WhenThenExpr{{When: &RawExpr{Name: "w"}, Then: &RawExpr{Name: "t"}}},
		&RawExpr{Name: "e"},
	)
	if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, complex); err != nil {
		t.Fatalf("Walk failed on complex case expr: %v", err)
	}

	var nilAlias *AliasedExpr
	var nilRaw *RawExpr
	var nilSQLVal *SQLVal
	var nilStar *StarExpr
	nilExprs := []Expr{nilAlias, nilRaw, nilSQLVal, nilStar}
	for _, e := range nilExprs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic on nil expr %T: %v", e, r)
				}
			}()
			tb := newTB()
			e.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("replace panic on nil expr %T: %v", e, r)
				}
			}()
			_ = e.replace(&RawExpr{Name: "from"}, &RawExpr{Name: "to"})
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic on nil expr %T: %v", e, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, e); err != nil {
				t.Fatalf("Walk failed on nil expr %T: %v", e, err)
			}
		}()
	}
}

func TestTableExprRuntimeContracts_ZeroAndComposite(t *testing.T) {
	tables := []TableExpr{
		&JoinTableExpr{},
		&SubqueryTableExpr{},
		&TableNameExpr{},
	}

	for _, te := range tables {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic on table expr %T: %v", te, r)
				}
			}()
			tb := newTB()
			te.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic on table expr %T: %v", te, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, te); err != nil {
				t.Fatalf("Walk failed on table expr %T: %v", te, err)
			}
		}()
	}

	join := &JoinTableExpr{
		Left:      &TableNameExpr{DBName: "db1", TableName: "t1"},
		Right:     &SubqueryTableExpr{Query: &SelectStmt{}},
		Condition: &RawExpr{Name: "cond"},
	}
	if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, join); err != nil {
		t.Fatalf("Walk failed on composite join expr: %v", err)
	}
}
