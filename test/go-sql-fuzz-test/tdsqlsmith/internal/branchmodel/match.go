package branchmodel

import (
	"fmt"
	"strconv"
	"strings"

	"sqlparser"
)

func MatchPositive(stmt sqlparser.Statement, keySpec string) error {
	sel, ok := stmt.(*sqlparser.SelectStmt)
	if !ok {
		return fmt.Errorf("expected *SelectStmt, got %T", stmt)
	}
	if keySpec == "" {
		return nil
	}
	parts := strings.Split(keySpec, ";")
	for _, p := range parts {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid key assertion %q", p)
		}
		if err := assertSelectKey(sel, kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

func assertSelectKey(s *sqlparser.SelectStmt, key string, val string) error {
	switch key {
	case "hint":
		if got := hintNameFromType(s.Hint); got != val {
			return mismatch(key, got, val)
		}
	case "distinct":
		want := val == "true"
		if s.IsDistinct != want {
			return mismatch(key, s.IsDistinct, want)
		}
	case "select_len":
		want, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		if len(s.Select) != want {
			return mismatch(key, len(s.Select), want)
		}
	case "from_kind":
		got := tableExprKind(s.From)
		if got != val {
			return mismatch(key, got, val)
		}
	case "join_type":
		j := firstJoin(s.From)
		if j == nil {
			return fmt.Errorf("join_type asserted but no join in FROM")
		}
		if got := joinTypeName(j.JoinType); got != val {
			return mismatch(key, got, val)
		}
	case "join_has_on":
		j := firstJoin(s.From)
		if j == nil {
			return fmt.Errorf("join_has_on asserted but no join in FROM")
		}
		want := val == "true"
		_, got := j.Condition.(sqlparser.Expr)
		if got != want {
			return mismatch(key, got, want)
		}
	case "join_has_jlimit":
		j := firstJoin(s.From)
		if j == nil {
			return fmt.Errorf("join_has_jlimit asserted but no join in FROM")
		}
		want := val == "true"
		got := j.JLimit != nil
		if got != want {
			return mismatch(key, got, want)
		}
	case "has_where":
		want := val == "true"
		got := s.Where != nil
		if got != want {
			return mismatch(key, got, want)
		}
	case "has_group_by":
		want := val == "true"
		got := s.GroupBy != nil && len(s.GroupBy.Exprs) > 0
		if got != want {
			return mismatch(key, got, want)
		}
	case "has_having":
		want := val == "true"
		got := s.Having != nil
		if got != want {
			return mismatch(key, got, want)
		}
	case "order_len":
		want, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		if len(s.OrderBy) != want {
			return mismatch(key, len(s.OrderBy), want)
		}
	case "set_op":
		if s.SetOp != val {
			return mismatch(key, s.SetOp, val)
		}
	case "set_all":
		want := val == "true"
		if s.SetAll != want {
			return mismatch(key, s.SetAll, want)
		}
	case "expr_kind":
		if len(s.Select) == 0 {
			return fmt.Errorf("expr_kind asserted but empty select list")
		}
		r := firstRaw(s.Select[0])
		if r == nil {
			return fmt.Errorf("expr_kind asserted but first expr is not raw")
		}
		if r.Kind != val {
			return mismatch(key, r.Kind, val)
		}
	case "range_kind":
		r := firstRaw(s.Range)
		if r == nil {
			return fmt.Errorf("range_kind asserted but range is nil/non-raw")
		}
		if r.Kind != val {
			return mismatch(key, r.Kind, val)
		}
	case "has_partition":
		want := val == "true"
		got := s.Partition != nil
		if got != want {
			return mismatch(key, got, want)
		}
	case "window_mode":
		got := ""
		switch {
		case len(s.Window.Interval.Val.Bytes) > 0:
			got = "interval"
		case s.Window.Session != nil:
			got = "session"
		case s.Window.StateWindow != nil:
			got = "state"
		case s.Window.EventWindowStart != nil || s.Window.EventWindowEnd != nil:
			got = "event"
		case len(s.Window.CountWindow.Bytes) > 0:
			got = "count"
		case s.Window.AnomalyWindow != nil:
			got = "anomaly"
		}
		if got != val {
			return mismatch(key, got, val)
		}
	case "count_cols_len":
		want, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		if len(s.Window.CountWindowCols) != want {
			return mismatch(key, len(s.Window.CountWindowCols), want)
		}
	case "select_nodes":
		want, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		if got := countSelectNodes(s); got != want {
			return mismatch(key, got, want)
		}
	case "from_subquery_depth":
		want, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		if got := fromSubqueryDepth(s.From); got != want {
			return mismatch(key, got, want)
		}
	case "first_select_is_subquery":
		want := val == "true"
		got := false
		if len(s.Select) > 0 {
			_, got = s.Select[0].(*sqlparser.SelectStmt)
		}
		if got != want {
			return mismatch(key, got, want)
		}
	case "where_has_subquery":
		want := val == "true"
		if got := exprHasSubquery(s.Where); got != want {
			return mismatch(key, got, want)
		}
	case "join_left_subquery":
		j, ok := s.From.(*sqlparser.JoinTableExpr)
		if !ok {
			return fmt.Errorf("join_left_subquery asserted but FROM is %T", s.From)
		}
		_, got := j.Left.(*sqlparser.SubqueryTableExpr)
		want := val == "true"
		if got != want {
			return mismatch(key, got, want)
		}
	case "join_right_subquery":
		j, ok := s.From.(*sqlparser.JoinTableExpr)
		if !ok {
			return fmt.Errorf("join_right_subquery asserted but FROM is %T", s.From)
		}
		_, got := j.Right.(*sqlparser.SubqueryTableExpr)
		want := val == "true"
		if got != want {
			return mismatch(key, got, want)
		}
	case "from_set_op":
		sub := firstFromSubquerySelect(s)
		if sub == nil {
			return fmt.Errorf("from_set_op asserted but no FROM subquery")
		}
		if sub.SetOp != val {
			return mismatch(key, sub.SetOp, val)
		}
	case "from_set_all":
		sub := firstFromSubquerySelect(s)
		if sub == nil {
			return fmt.Errorf("from_set_all asserted but no FROM subquery")
		}
		want := val == "true"
		if sub.SetAll != want {
			return mismatch(key, sub.SetAll, want)
		}
	default:
		return fmt.Errorf("unsupported select key assertion %q", key)
	}
	return nil
}

func mismatch(key string, got any, want any) error {
	return fmt.Errorf("%s mismatch: got=%v want=%v", key, got, want)
}

func hintNameFromType(h *sqlparser.HintOption) string {
	if h == nil {
		return ""
	}
	switch h.HintType {
	case sqlparser.HINT_BATCH_SCAN:
		return "batch_scan"
	case sqlparser.HINT_NO_BATCH_SCAN:
		return "no_batch_scan"
	case sqlparser.HINT_HASH_JOIN:
		return "hash_join"
	case sqlparser.HINT_SORT_FOR_GROUP:
		return "sort_for_group"
	case sqlparser.HINT_PARTITION_FIRST:
		return "partition_first"
	case sqlparser.HINT_PARA_TABLES_SORT:
		return "para_tables_sort"
	case sqlparser.HINT_SMALLDATA_TS_SORT:
		return "smalldata_ts_sort"
	case sqlparser.HINT_SKIP_TSMA:
		return "skip_tsma"
	case sqlparser.HINT_WIN_OPTIMIZE_BATCH:
		return "win_optimize_batch"
	case sqlparser.HINT_WIN_OPTIMIZE_SINGLE:
		return "win_optimize_single"
	default:
		return ""
	}
}

func joinTypeName(j sqlparser.JoinType) string {
	switch j {
	case sqlparser.JoinTypeInner:
		return "inner"
	case sqlparser.JoinTypeLeft:
		return "left"
	case sqlparser.JoinTypeRight:
		return "right"
	case sqlparser.JoinTypeFull:
		return "full"
	case sqlparser.JoinTypeLeftSemi:
		return "left_semi"
	case sqlparser.JoinTypeRightSemi:
		return "right_semi"
	case sqlparser.JoinTypeLeftAnti:
		return "left_anti"
	case sqlparser.JoinTypeRightAnti:
		return "right_anti"
	case sqlparser.JoinTypeLeftAsof:
		return "left_asof"
	case sqlparser.JoinTypeRightAsof:
		return "right_asof"
	case sqlparser.JoinTypeLeftWindow:
		return "left_window"
	case sqlparser.JoinTypeRightWindow:
		return "right_window"
	default:
		return ""
	}
}

func firstJoin(from sqlparser.TableExpr) *sqlparser.JoinTableExpr {
	switch x := from.(type) {
	case *sqlparser.JoinTableExpr:
		return x
	case *sqlparser.SubqueryTableExpr:
		if x.Query == nil {
			return nil
		}
		return firstJoin(x.Query.From)
	default:
		return nil
	}
}

func firstRaw(expr sqlparser.Expr) *sqlparser.RawExpr {
	switch x := expr.(type) {
	case *sqlparser.RawExpr:
		return x
	case *sqlparser.AliasedExpr:
		return firstRaw(x.Expr)
	default:
		return nil
	}
}

func tableExprKind(t sqlparser.TableExpr) string {
	switch t.(type) {
	case *sqlparser.TableNameExpr:
		return "table"
	case *sqlparser.SubqueryTableExpr:
		return "subquery"
	case *sqlparser.JoinTableExpr:
		return "join"
	case nil:
		return "nil"
	default:
		return "other"
	}
}

func countSelectNodes(root sqlparser.SQLNode) int {
	if root == nil {
		return 0
	}
	seen := map[*sqlparser.SelectStmt]struct{}{}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		s, ok := node.(*sqlparser.SelectStmt)
		if ok && s != nil {
			seen[s] = struct{}{}
		}
		return true, nil
	}, root)
	return len(seen)
}

func fromSubqueryDepth(t sqlparser.TableExpr) int {
	switch x := t.(type) {
	case *sqlparser.SubqueryTableExpr:
		if x.Query == nil {
			return 1
		}
		return 1 + fromSubqueryDepth(x.Query.From)
	case *sqlparser.JoinTableExpr:
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

func exprHasSubquery(e sqlparser.Expr) bool {
	if e == nil {
		return false
	}
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if s, ok := node.(*sqlparser.SelectStmt); ok && s != nil {
			has = true
		}
		return true, nil
	}, e)
	return has
}

func firstFromSubquerySelect(s *sqlparser.SelectStmt) *sqlparser.SelectStmt {
	if s == nil {
		return nil
	}
	switch x := s.From.(type) {
	case *sqlparser.SubqueryTableExpr:
		return x.Query
	case *sqlparser.JoinTableExpr:
		if l, ok := x.Left.(*sqlparser.SubqueryTableExpr); ok {
			return l.Query
		}
		if r, ok := x.Right.(*sqlparser.SubqueryTableExpr); ok {
			return r.Query
		}
	}
	return nil
}
