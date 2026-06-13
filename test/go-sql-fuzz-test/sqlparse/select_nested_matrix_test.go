package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type selectNestedCase struct {
	id         string
	nestedKind string
	sql        string
	keyAssert  string
}

func loadSelectNestedCases(t *testing.T) []selectNestedCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "select_nested_matrix.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open select nested matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	out := make([]selectNestedCase, 0, 64)
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			t.Fatalf("invalid select nested matrix line %d: %q", line, s)
		}
		out = append(out, selectNestedCase{
			id:         cols[0],
			nestedKind: cols[1],
			sql:        cols[2],
			keyAssert:  cols[3],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan select nested matrix failed: %v", err)
	}
	return out
}

func tableExprKind(t TableExpr) string {
	switch t.(type) {
	case *TableNameExpr:
		return "table"
	case *SubqueryTableExpr:
		return "subquery"
	case *JoinTableExpr:
		return "join"
	case nil:
		return "nil"
	default:
		return "other"
	}
}

func countSelectNodes(root SQLNode) int {
	if root == nil {
		return 0
	}
	seen := map[*SelectStmt]struct{}{}
	_ = Walk(func(node SQLNode) (bool, error) {
		if s, ok := node.(*SelectStmt); ok && s != nil {
			seen[s] = struct{}{}
		}
		return true, nil
	}, root)
	return len(seen)
}

func fromSubqueryDepth(t TableExpr) int {
	switch x := t.(type) {
	case *SubqueryTableExpr:
		if x.Query == nil {
			return 1
		}
		return 1 + fromSubqueryDepth(x.Query.From)
	case *JoinTableExpr:
		ld := fromSubqueryDepth(x.Left)
		rd := fromSubqueryDepth(x.Right)
		if ld > rd {
			return ld
		}
		return rd
	default:
		return 0
	}
}

func exprHasSubquery(e Expr) bool {
	if e == nil {
		return false
	}
	has := false
	_ = Walk(func(node SQLNode) (bool, error) {
		if s, ok := node.(*SelectStmt); ok && s != nil {
			has = true
		}
		return true, nil
	}, e)
	return has
}

func firstFromSubquerySelect(s *SelectStmt) *SelectStmt {
	if s == nil {
		return nil
	}
	switch x := s.From.(type) {
	case *SubqueryTableExpr:
		return x.Query
	case *JoinTableExpr:
		if l, ok := x.Left.(*SubqueryTableExpr); ok {
			return l.Query
		}
		if r, ok := x.Right.(*SubqueryTableExpr); ok {
			return r.Query
		}
	}
	return nil
}

func parseBoolValue(t *testing.T, in string) bool {
	t.Helper()
	switch in {
	case "true":
		return true
	case "false":
		return false
	default:
		t.Fatalf("invalid bool assertion value %q", in)
		return false
	}
}

func assertSelectNestedKeyFields(t *testing.T, s *SelectStmt, keySpec string) {
	t.Helper()
	if keySpec == "" {
		return
	}
	parts := strings.Split(keySpec, ";")
	for _, p := range parts {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			t.Fatalf("invalid key assertion %q", p)
		}
		k, v := kv[0], kv[1]
		switch k {
		case "select_nodes":
			want, err := strconv.Atoi(v)
			if err != nil {
				t.Fatalf("invalid select_nodes value %q: %v", v, err)
			}
			if got := countSelectNodes(s); got != want {
				t.Fatalf("select_nodes mismatch: got=%d want=%d", got, want)
			}
		case "from_kind":
			if got := tableExprKind(s.From); got != v {
				t.Fatalf("from_kind mismatch: got=%q want=%q", got, v)
			}
		case "from_subquery_depth":
			want, err := strconv.Atoi(v)
			if err != nil {
				t.Fatalf("invalid from_subquery_depth value %q: %v", v, err)
			}
			if got := fromSubqueryDepth(s.From); got != want {
				t.Fatalf("from_subquery_depth mismatch: got=%d want=%d", got, want)
			}
		case "first_select_is_subquery":
			want := parseBoolValue(t, v)
			got := false
			if len(s.Select) > 0 {
				_, got = s.Select[0].(*SelectStmt)
			}
			if got != want {
				t.Fatalf("first_select_is_subquery mismatch: got=%v want=%v", got, want)
			}
		case "has_where":
			want := parseBoolValue(t, v)
			if got := s.Where != nil; got != want {
				t.Fatalf("has_where mismatch: got=%v want=%v", got, want)
			}
		case "where_has_subquery":
			want := parseBoolValue(t, v)
			if got := exprHasSubquery(s.Where); got != want {
				t.Fatalf("where_has_subquery mismatch: got=%v want=%v", got, want)
			}
		case "join_left_subquery":
			j, ok := s.From.(*JoinTableExpr)
			if !ok {
				t.Fatalf("join_left_subquery asserted but from is %T", s.From)
			}
			_, got := j.Left.(*SubqueryTableExpr)
			want := parseBoolValue(t, v)
			if got != want {
				t.Fatalf("join_left_subquery mismatch: got=%v want=%v", got, want)
			}
		case "join_right_subquery":
			j, ok := s.From.(*JoinTableExpr)
			if !ok {
				t.Fatalf("join_right_subquery asserted but from is %T", s.From)
			}
			_, got := j.Right.(*SubqueryTableExpr)
			want := parseBoolValue(t, v)
			if got != want {
				t.Fatalf("join_right_subquery mismatch: got=%v want=%v", got, want)
			}
		case "set_op":
			if s.SetOp != v {
				t.Fatalf("set_op mismatch: got=%q want=%q", s.SetOp, v)
			}
		case "set_all":
			want := parseBoolValue(t, v)
			if s.SetAll != want {
				t.Fatalf("set_all mismatch: got=%v want=%v", s.SetAll, want)
			}
		case "from_set_op":
			sub := firstFromSubquerySelect(s)
			if sub == nil {
				t.Fatalf("from_set_op asserted but from has no subquery")
			}
			if sub.SetOp != v {
				t.Fatalf("from_set_op mismatch: got=%q want=%q", sub.SetOp, v)
			}
		case "from_set_all":
			sub := firstFromSubquerySelect(s)
			if sub == nil {
				t.Fatalf("from_set_all asserted but from has no subquery")
			}
			want := parseBoolValue(t, v)
			if sub.SetAll != want {
				t.Fatalf("from_set_all mismatch: got=%v want=%v", sub.SetAll, want)
			}
		default:
			t.Fatalf("unsupported nested key assertion %q", k)
		}
	}
}

func TestSelectNestedMatrix_RoundTrip(t *testing.T) {
	cases := loadSelectNestedCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty select nested matrix")
	}

	covered := map[string]struct{}{}
	for _, tc := range cases {
		tc := tc
		covered[tc.nestedKind] = struct{}{}
		t.Run(tc.id, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("[%s] parse failed: %v sql=%q", tc.id, err, tc.sql)
			}
			sel, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("[%s] expected *SelectStmt, got %T", tc.id, stmt)
			}

			assertSelectNestedKeyFields(t, sel, tc.keyAssert)
			runStatementRoundTrip(t, tc.sql)
		})
	}

	requiredKinds := []string{
		"from_subquery_basic",
		"from_subquery_double_paren",
		"from_subquery_chain",
		"from_subquery_deep_chain",
		"scalar_subquery_select_item",
		"scalar_subquery_nested",
		"where_scalar_subquery_cmp",
		"where_scalar_subquery_arith",
		"where_subquery_is_null",
		"from_subquery_union_all",
		"from_subquery_order_limit",
		"from_subquery_group_having",
		"join_both_subqueries",
		"join_left_subquery",
		"join_right_subquery",
		"subquery_in_cast",
		"subquery_in_coalesce",
		"subquery_in_if",
		"subquery_in_case_when",
		"subquery_statement_parenthesized",
		"subquery_statement_double_parenthesized",
		"subquery_with_partition",
		"subquery_with_window",
		"top_level_union_parenthesized",
	}
	for _, k := range requiredKinds {
		if _, ok := covered[k]; !ok {
			t.Fatalf("required nested kind not covered: %s", k)
		}
	}
}
