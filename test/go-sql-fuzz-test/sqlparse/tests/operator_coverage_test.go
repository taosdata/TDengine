package sqlparser_test

import (
	"sort"
	"testing"

	"sqlparser"
)

func collectOperatorsFromStatement(stmt sqlparser.Statement, out map[string]struct{}) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		raw, ok := node.(*sqlparser.RawExpr)
		if !ok || raw == nil {
			return true, nil
		}
		if len(raw.Op.Bytes) > 0 {
			out[string(raw.Op.Bytes)] = struct{}{}
		}
		return true, nil
	}, stmt)
}

func TestOperatorCoverage_RealSQLParse_AllOperators(t *testing.T) {
	cases := []string{
		"select +1 from t1;",
		"select -1 from t1;",
		"select 1 + 2 from t1;",
		"select 3 - 2 from t1;",
		"select 2 * 3 from t1;",
		"select 8 / 2 from t1;",
		"select 5 % 2 from t1;",
		"select 1 & 3 from t1;",
		"select 1 | 2 from t1;",
		"select not (v > 1) from t1;",
		"select v > 1 or v < 2 from t1;",
		"select v > 1 and v < 2 from t1;",
		"select v < 1 from t1;",
		"select v > 1 from t1;",
		"select v <= 1 from t1;",
		"select v >= 1 from t1;",
		"select v != 1 from t1;",
		"select v = 1 from t1;",
		"select v like 'a%' from t1;",
		"select v not like 'a%' from t1;",
		"select v match 'a.*' from t1;",
		"select v nmatch 'a.*' from t1;",
		"select v regexp 'a.*' from t1;",
		"select v not regexp 'a.*' from t1;",
		"select j contains 'k' from t1;",
		"select v in (1,2) from t1;",
		"select v not in (1,2) from t1;",
	}

	expected := map[string]struct{}{
		"uplus":    {},
		"minus":    {},
		"add":      {},
		"sub":      {},
		"mul":      {},
		"div":      {},
		"rem":      {},
		"bit_and":  {},
		"bit_or":   {},
		"not":      {},
		"or":       {},
		"and":      {},
		"lt":       {},
		"gt":       {},
		"le":       {},
		"ge":       {},
		"ne":       {},
		"eq":       {},
		"like":     {},
		"not_like": {},
		"match":    {},
		"nmatch":   {},
		"contains": {},
		"in":       {},
		"not_in":   {},
	}

	hit := map[string]struct{}{}
	for _, sql := range cases {
		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
		if _, ok := stmt.(*sqlparser.SelectStmt); !ok {
			t.Fatalf("expected *SelectStmt for %q, got %T", sql, stmt)
		}
		if err := collectOperatorsFromStatement(stmt, hit); err != nil {
			t.Fatalf("walk failed for %q: %v", sql, err)
		}
	}

	var missing []string
	for op := range expected {
		if _, ok := hit[op]; !ok {
			missing = append(missing, op)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("real-sql operator coverage missing: %v", missing)
	}
}
