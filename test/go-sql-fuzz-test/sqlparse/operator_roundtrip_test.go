package sqlparser

import (
	"fmt"
	"strings"
	"testing"
)

func TestOperatorRoundTrip_PreserveGrouping(t *testing.T) {
	cases := []string{
		"select (a + b) * c from t;",
		"select not (a > 1 and b > 2) from t;",
		"select * from t where (a + b) * c > 1;",
		"select * from t where (a between 1 and 2) and c = 1;",
		"select * from t where (a = 1 or b = 2) and c = 3;",
	}
	for i, sql := range cases {
		t.Run(fmt.Sprintf("operator_grouping_%02d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}

func TestOperatorRoundTrip_Matrix(t *testing.T) {
	cases := []string{
		"select a + b from t;",
		"select a - b from t;",
		"select a * b from t;",
		"select a / b from t;",
		"select a % b from t;",
		"select a & b from t;",
		"select a | b from t;",
		"select +a from t;",
		"select -a from t;",
		"select a = b from t;",
		"select a != b from t;",
		"select a < b from t;",
		"select a <= b from t;",
		"select a > b from t;",
		"select a >= b from t;",
		"select a like 'x%' from t;",
		"select a not like 'x%' from t;",
		"select a match 'x' from t;",
		"select a nmatch 'x' from t;",
		"select a regexp 'x' from t;",
		"select a not regexp 'x' from t;",
		"select a contains 'x' from t;",
		"select a between 1 and 2 from t;",
		"select a not between 1 and 2 from t;",
		"select a is null from t;",
		"select a is not null from t;",
		"select isnull(a) from t;",
		"select isnotnull(a) from t;",
		"select a in (1) from t;",
		"select a in (1, 2, 3) from t;",
		"select a not in (1, 2, 3) from t;",
		"select (a->'x') = '1' from t;",
		"select ((a + b) * c) > 1 from t;",
	}
	for i, sql := range cases {
		t.Run(fmt.Sprintf("operator_matrix_%02d", i+1), func(t *testing.T) {
			runStatementRoundTrip(t, sql)
		})
	}
}

func TestRawExprFormat_FuncNilArg(t *testing.T) {
	expr := &RawExpr{Kind: "func", Name: "f", Args: []Expr{nil}}
	tb := newTB()
	expr.Format(tb)
	if got := tb.String(); got != "f()" {
		t.Fatalf("unexpected format output: %q", got)
	}
}

func TestRawExprFormat_BetweenPointerExtra(t *testing.T) {
	expr := &RawExpr{
		Kind: "between",
		Left: &RawExpr{Kind: "col", Name: "a"},
		Extra: &betweenExtra{
			From: &RawExpr{Kind: "col", Name: "b"},
			To:   &RawExpr{Kind: "col", Name: "c"},
			Not:  true,
		},
	}
	tb := newTB()
	expr.Format(tb)
	if got := tb.String(); got != "a not between b and c" {
		t.Fatalf("unexpected format output: %q", got)
	}
}

func TestRawExprFormat_BetweenNilPointerExtra(t *testing.T) {
	expr := &RawExpr{
		Kind:  "between",
		Left:  &RawExpr{Kind: "col", Name: "a"},
		Extra: (*betweenExtra)(nil),
	}
	tb := newTB()
	expr.Format(tb)
	if got := tb.String(); got != "a" {
		t.Fatalf("unexpected format output: %q", got)
	}
}

func TestCompareOp_RegexpAliasesMapToMatch(t *testing.T) {
	cases := []struct {
		sql    string
		wantOp string
		wantIn string
	}{
		{
			sql:    "select a regexp 'x' from t;",
			wantOp: "match",
			wantIn: " match ",
		},
		{
			sql:    "select a not regexp 'x' from t;",
			wantOp: "nmatch",
			wantIn: " nmatch ",
		},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("regexp_alias_%02d", i+1), func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			sel, ok := stmt.(*SelectStmt)
			if !ok || len(sel.Select) != 1 {
				t.Fatalf("unexpected statement shape: %T %#v", stmt, stmt)
			}
			re, ok := sel.Select[0].(*RawExpr)
			if !ok || re.Kind != "cmp" {
				t.Fatalf("unexpected select expr: %T %#v", sel.Select[0], sel.Select[0])
			}
			if got := string(re.Op.Bytes); got != tc.wantOp {
				t.Fatalf("compare op mismatch: got=%q want=%q", got, tc.wantOp)
			}
			formatted := SQLNodeToString(stmt)
			if !strings.Contains(formatted, tc.wantIn) {
				t.Fatalf("formatted sql mismatch: got=%q want contain=%q", formatted, tc.wantIn)
			}
		})
	}
}

func TestInPredicateValue_TimestampLiteralPreserved(t *testing.T) {
	sql := "select ts in (timestamp '2020-01-01 00:00:00', timestamp '2020-01-02 00:00:00') from t;"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	formatted := SQLNodeToString(stmt)
	if !strings.Contains(formatted, "timestamp '2020-01-01 00:00:00'") {
		t.Fatalf("formatted sql missing timestamp literal: %q", formatted)
	}
	if !strings.Contains(formatted, "timestamp '2020-01-02 00:00:00'") {
		t.Fatalf("formatted sql missing timestamp literal: %q", formatted)
	}
	runStatementRoundTrip(t, sql)
}

func TestInPredicateValue_SignedIntegerPlusCanonicalized(t *testing.T) {
	sql := "select v in (-1, +2, +3) from t;"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	formatted := SQLNodeToString(stmt)
	if strings.Contains(formatted, "+2") || strings.Contains(formatted, "+3") {
		t.Fatalf("signed plus integer should be canonicalized: %q", formatted)
	}
	if !strings.Contains(formatted, "in (-1, 2, 3)") {
		t.Fatalf("unexpected in-list format: %q", formatted)
	}
	runStatementRoundTrip(t, sql)
}

func TestInPredicateValue_SignedFloatPlusCanonicalized(t *testing.T) {
	sql := "select v in (+2.5, -3.5, +0.25) from t;"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	formatted := SQLNodeToString(stmt)
	if strings.Contains(formatted, "+2.5") || strings.Contains(formatted, "+0.25") {
		t.Fatalf("signed plus float should be canonicalized: %q", formatted)
	}
	if !strings.Contains(formatted, "in (2.5, -3.5, 0.25)") {
		t.Fatalf("unexpected in-list format: %q", formatted)
	}
	runStatementRoundTrip(t, sql)
}
