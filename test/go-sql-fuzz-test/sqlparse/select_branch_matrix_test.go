package sqlparser

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type selectBranchCase struct {
	id        string
	rule      string
	branchSig string
	sql       string
	keyAssert string
}

func loadSelectBranchCases(t *testing.T) []selectBranchCase {
	t.Helper()
	path := filepath.Join("testdata", "sql_corpus", "select_branch_matrix.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open select branch matrix failed: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	var out []selectBranchCase
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 5 {
			t.Fatalf("invalid select branch matrix line %d: %q", line, s)
		}
		out = append(out, selectBranchCase{
			id:        cols[0],
			rule:      cols[1],
			branchSig: cols[2],
			sql:       cols[3],
			keyAssert: cols[4],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan select branch matrix failed: %v", err)
	}
	return out
}

func hintNameFromType(h *HintOption) string {
	if h == nil {
		return ""
	}
	switch h.HintType {
	case HINT_BATCH_SCAN:
		return "batch_scan"
	case HINT_NO_BATCH_SCAN:
		return "no_batch_scan"
	case HINT_HASH_JOIN:
		return "hash_join"
	case HINT_SORT_FOR_GROUP:
		return "sort_for_group"
	case HINT_PARTITION_FIRST:
		return "partition_first"
	case HINT_PARA_TABLES_SORT:
		return "para_tables_sort"
	case HINT_SMALLDATA_TS_SORT:
		return "smalldata_ts_sort"
	case HINT_SKIP_TSMA:
		return "skip_tsma"
	case HINT_WIN_OPTIMIZE_BATCH:
		return "win_optimize_batch"
	case HINT_WIN_OPTIMIZE_SINGLE:
		return "win_optimize_single"
	default:
		return ""
	}
}

func joinTypeName(j JoinType) string {
	switch j {
	case JoinTypeInner:
		return "inner"
	case JoinTypeLeft:
		return "left"
	case JoinTypeRight:
		return "right"
	case JoinTypeFull:
		return "full"
	case JoinTypeLeftSemi:
		return "left_semi"
	case JoinTypeRightSemi:
		return "right_semi"
	case JoinTypeLeftAnti:
		return "left_anti"
	case JoinTypeRightAnti:
		return "right_anti"
	case JoinTypeLeftAsof:
		return "left_asof"
	case JoinTypeRightAsof:
		return "right_asof"
	case JoinTypeLeftWindow:
		return "left_window"
	case JoinTypeRightWindow:
		return "right_window"
	default:
		return ""
	}
}

func firstJoin(from TableExpr) *JoinTableExpr {
	switch x := from.(type) {
	case *JoinTableExpr:
		return x
	case *SubqueryTableExpr:
		return firstJoin(x.Query.From)
	default:
		return nil
	}
}

func firstRaw(expr Expr) *RawExpr {
	switch x := expr.(type) {
	case *RawExpr:
		return x
	case *AliasedExpr:
		return firstRaw(x.Expr)
	default:
		return nil
	}
}

func assertSelectKeyFields(t *testing.T, s *SelectStmt, keySpec string) {
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
		case "hint":
			if got := hintNameFromType(s.Hint); got != v {
				t.Fatalf("hint mismatch: got=%q want=%q", got, v)
			}
		case "distinct":
			want := v == "true"
			if s.IsDistinct != want {
				t.Fatalf("distinct mismatch: got=%v want=%v", s.IsDistinct, want)
			}
		case "select_len":
			want, _ := strconv.Atoi(v)
			if len(s.Select) != want {
				t.Fatalf("select_len mismatch: got=%d want=%d", len(s.Select), want)
			}
		case "from_kind":
			got := "nil"
			switch s.From.(type) {
			case *TableNameExpr:
				got = "table"
			case *JoinTableExpr:
				got = "join"
			case *SubqueryTableExpr:
				got = "subquery"
			}
			if got != v {
				t.Fatalf("from_kind mismatch: got=%q want=%q", got, v)
			}
		case "join_type":
			j := firstJoin(s.From)
			if j == nil {
				t.Fatalf("join_type asserted but no join in from")
			}
			if got := joinTypeName(j.JoinType); got != v {
				t.Fatalf("join_type mismatch: got=%q want=%q", got, v)
			}
		case "join_has_on":
			j := firstJoin(s.From)
			if j == nil {
				t.Fatalf("join_has_on asserted but no join in from")
			}
			want := v == "true"
			got := false
			if condExpr, ok := j.Condition.(Expr); ok && condExpr != nil {
				got = true
			}
			if got != want {
				t.Fatalf("join_has_on mismatch: got=%v want=%v", got, want)
			}
		case "join_has_window_offset":
			j := firstJoin(s.From)
			if j == nil {
				t.Fatalf("join_has_window_offset asserted but no join in from")
			}
			want := v == "true"
			got := j.WindowOffset != nil
			if got != want {
				t.Fatalf("join_has_window_offset mismatch: got=%v want=%v", got, want)
			}
		case "join_has_jlimit":
			j := firstJoin(s.From)
			if j == nil {
				t.Fatalf("join_has_jlimit asserted but no join in from")
			}
			want := v == "true"
			got := j.JLimit != nil
			if got != want {
				t.Fatalf("join_has_jlimit mismatch: got=%v want=%v", got, want)
			}
		case "has_where":
			want := v == "true"
			if (s.Where != nil) != want {
				t.Fatalf("has_where mismatch")
			}
		case "has_group_by":
			want := v == "true"
			got := s.GroupBy != nil && len(s.GroupBy.Exprs) > 0
			if got != want {
				t.Fatalf("has_group_by mismatch")
			}
		case "has_having":
			want := v == "true"
			if (s.Having != nil) != want {
				t.Fatalf("has_having mismatch")
			}
		case "order_len":
			want, _ := strconv.Atoi(v)
			if len(s.OrderBy) != want {
				t.Fatalf("order_len mismatch: got=%d want=%d", len(s.OrderBy), want)
			}
		case "limit":
			if s.Limit == nil {
				t.Fatalf("limit asserted but limit is nil")
			}
			if got := string(s.Limit.Limit.Bytes); got != v {
				t.Fatalf("limit mismatch: got=%q want=%q", got, v)
			}
		case "offset":
			if s.Limit == nil {
				t.Fatalf("offset asserted but limit is nil")
			}
			if got := string(s.Limit.Offset.Bytes); got != v {
				t.Fatalf("offset mismatch: got=%q want=%q", got, v)
			}
		case "slimit":
			if s.SLimit == nil {
				t.Fatalf("slimit asserted but slimit is nil")
			}
			if got := string(s.SLimit.SLimit.Bytes); got != v {
				t.Fatalf("slimit mismatch: got=%q want=%q", got, v)
			}
		case "soffset":
			if s.SLimit == nil {
				t.Fatalf("soffset asserted but slimit is nil")
			}
			if got := string(s.SLimit.SOffset.Bytes); got != v {
				t.Fatalf("soffset mismatch: got=%q want=%q", got, v)
			}
		case "set_op":
			if s.SetOp != v {
				t.Fatalf("set_op mismatch: got=%q want=%q", s.SetOp, v)
			}
		case "set_all":
			want := v == "true"
			if s.SetAll != want {
				t.Fatalf("set_all mismatch: got=%v want=%v", s.SetAll, want)
			}
		case "expr_kind":
			if len(s.Select) == 0 {
				t.Fatalf("expr_kind asserted but select is empty")
			}
			r := firstRaw(s.Select[0])
			if r == nil {
				t.Fatalf("expr_kind asserted but first expr is not raw")
			}
			if r.Kind != v {
				t.Fatalf("expr_kind mismatch: got=%q want=%q", r.Kind, v)
			}
		case "range_kind":
			r := firstRaw(s.Range)
			if r == nil {
				t.Fatalf("range_kind asserted but range is nil/non-raw")
			}
			if r.Kind != v {
				t.Fatalf("range_kind mismatch: got=%q want=%q", r.Kind, v)
			}
		case "has_partition":
			want := v == "true"
			if (s.Partition != nil) != want {
				t.Fatalf("has_partition mismatch")
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
			if got != v {
				t.Fatalf("window_mode mismatch: got=%q want=%q", got, v)
			}
		case "window_has_true_for":
			want := v == "true"
			got := len(s.Window.TrueFor.Val.Bytes) > 0
			if got != want {
				t.Fatalf("window_has_true_for mismatch: got=%v want=%v", got, want)
			}
		case "count_cols_len":
			want, _ := strconv.Atoi(v)
			if len(s.Window.CountWindowCols) != want {
				t.Fatalf("count_cols_len mismatch: got=%d want=%d", len(s.Window.CountWindowCols), want)
			}
		default:
			t.Fatalf("unsupported select key assertion %q", k)
		}
	}
}

func TestSelectBranchMatrix_RoundTrip(t *testing.T) {
	cases := loadSelectBranchCases(t)
	if len(cases) == 0 {
		t.Fatalf("empty select branch matrix")
	}
	coveredRules := map[string]struct{}{}
	for _, tc := range cases {
		coveredRules[tc.rule] = struct{}{}
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("[%s] parse failed: %v sql=%q", tc.id, err, tc.sql)
		}
		sel, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("[%s] not select stmt: %T", tc.id, stmt)
		}
		assertSelectKeyFields(t, sel, tc.keyAssert)
		runStatementRoundTrip(t, tc.sql)
	}

	requiredRules := []string{
		"select_statement",
		"query_or_subquery",
		"query_expression",
		"query_simple",
		"union_query_expression",
		"query_simple_or_subquery",
		"query_specification",
		"hint_list",
		"set_quantifier_opt",
		"select_item",
		"from_clause_opt",
		"alias_opt",
		"table_primary",
		"joined_table",
		"join_on_clause_opt",
		"jlimit_clause_opt",
		"where_clause_opt",
		"group_by_clause_opt",
		"having_clause_opt",
		"range_opt",
		"every_opt",
		"order_by_clause_opt",
		"ordering_specification_opt",
		"null_ordering_opt",
		"limit_clause_opt",
		"slimit_clause_opt",
		"subquery",
		"partition_by_clause_opt",
		"twindow_clause_opt",
		"count_window_args",
		"predicate",
		"compare_op",
		"in_op",
		"function_expression",
	}
	for _, r := range requiredRules {
		if _, ok := coveredRules[r]; !ok {
			t.Fatalf("required select rule not covered in matrix: %s", r)
		}
	}
}
