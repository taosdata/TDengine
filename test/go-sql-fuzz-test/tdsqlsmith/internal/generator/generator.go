package generator

import (
	"fmt"
	"strconv"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/impedance"
	"tdsqlsmith/internal/random"
)

type Config struct {
	MutationLevel int
	WriteSQL      []string
	WriteRatio    int // 0..100
}

type Generated struct {
	CaseID  string
	Rule    string
	SQL     string
	Mutated bool
	Kind    string
}

type Generator struct {
	cfg   Config
	cases []branchmodel.PositiveCase
	byID  map[string]branchmodel.PositiveCase
	write []string
}

func New(cases []branchmodel.PositiveCase, cfg Config) (*Generator, error) {
	if len(cases) == 0 {
		return nil, fmt.Errorf("empty positive cases")
	}
	if cfg.MutationLevel < 0 || cfg.MutationLevel > 3 {
		return nil, fmt.Errorf("invalid mutation level %d", cfg.MutationLevel)
	}
	if cfg.WriteRatio < 0 || cfg.WriteRatio > 100 {
		return nil, fmt.Errorf("invalid write ratio %d", cfg.WriteRatio)
	}
	if cfg.WriteRatio == 0 {
		cfg.WriteRatio = 15
	}
	byID := make(map[string]branchmodel.PositiveCase, len(cases))
	for _, c := range cases {
		byID[c.ID] = c
	}
	return &Generator{
		cfg:   cfg,
		cases: append([]branchmodel.PositiveCase(nil), cases...),
		byID:  byID,
		write: append([]string(nil), cfg.WriteSQL...),
	}, nil
}

func (g *Generator) Next(r *random.RNG, missingIDs []string) (Generated, error) {
	if r == nil {
		return Generated{}, fmt.Errorf("nil random rng")
	}
	picked, err := g.pickCase(r, missingIDs)
	if err != nil {
		return Generated{}, err
	}
	if picked.ID == "" {
		return Generated{
			CaseID:  "",
			Rule:    "write_statement",
			SQL:     picked.SQL,
			Mutated: false,
			Kind:    "write",
		}, nil
	}

	sqlText := strings.TrimSpace(picked.SQL)
	if sqlText == "" {
		return Generated{}, fmt.Errorf("empty SQL for case %s", picked.ID)
	}
	missingSet := make(map[string]struct{}, len(missingIDs))
	for _, id := range missingIDs {
		missingSet[id] = struct{}{}
	}
	mutateProb := 100
	if _, ok := missingSet[picked.ID]; ok {
		// Prioritize deterministic hit for uncovered branches.
		mutateProb = 20
	}

	mutated := false
	if g.cfg.MutationLevel > 0 && r.Intn(100) < mutateProb {
		sqlText, mutated = mutateSQL(sqlText, g.cfg.MutationLevel, r)
	}
	return Generated{
		CaseID:  picked.ID,
		Rule:    picked.Rule,
		SQL:     sqlText,
		Mutated: mutated,
		Kind:    "query",
	}, nil
}

func (g *Generator) pickCase(r *random.RNG, missingIDs []string) (branchmodel.PositiveCase, error) {
	if len(missingIDs) == 0 && len(g.write) > 0 && r.Intn(100) < g.cfg.WriteRatio {
		sqlText := strings.TrimSpace(g.write[r.Intn(len(g.write))])
		if sqlText != "" {
			return branchmodel.PositiveCase{SQL: sqlText}, nil
		}
	}
	if len(missingIDs) > 0 && r.Intn(100) < 85 {
		id := missingIDs[r.Intn(len(missingIDs))]
		if c, ok := g.byID[id]; ok && impedance.Matched(c.Rule) {
			return c, nil
		}
	}
	if len(g.cases) == 0 {
		return branchmodel.PositiveCase{}, fmt.Errorf("no cases available")
	}

	// Avoid high-failure-rate productions when possible.
	for tries := 0; tries < 32; tries++ {
		c := g.cases[r.Intn(len(g.cases))]
		if impedance.Matched(c.Rule) {
			return c, nil
		}
	}
	return g.cases[r.Intn(len(g.cases))], nil
}

func mutateSQL(sqlText string, level int, r *random.RNG) (string, bool) {
	original := strings.TrimSpace(sqlText)
	if original == "" || level <= 0 || r == nil {
		return original, false
	}
	out := original
	mutated := false

	if level >= 1 && r.Intn(100) < 60 {
		if rewritten, ok := rewriteLiteral(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 2 && r.Intn(100) < 45 {
		if rewritten, ok := rewriteComparison(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 2 && r.Intn(100) < 35 {
		if rewritten, ok := rewriteIsNull(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 3 && r.Intn(100) < 30 {
		if rewritten, ok := rewriteUnion(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 3 && r.Intn(100) < 30 {
		if rewritten, ok := rewriteJoinType(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 3 && r.Intn(100) < 30 {
		if rewritten, ok := rewriteLimit(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if level >= 3 && r.Intn(100) < 30 {
		if rewritten, ok := rewriteFillMode(out, r); ok {
			out = rewritten
			mutated = true
		}
	}
	if !mutated && level >= 3 {
		if rewritten, ok := structuredRewrite(out, r); ok {
			out = rewritten
			mutated = true
		}
	}

	out = strings.TrimSpace(out)
	if out == "" {
		return original, false
	}
	return out, mutated && out != original
}

func structuredRewrite(in string, r *random.RNG) (string, bool) {
	rewriters := []func(string, *random.RNG) (string, bool){
		rewriteLiteral,
		rewriteComparison,
		rewriteIsNull,
		rewriteUnion,
		rewriteJoinType,
		rewriteLimit,
		rewriteFillMode,
	}
	start := r.Intn(len(rewriters))
	for i := 0; i < len(rewriters); i++ {
		idx := (start + i) % len(rewriters)
		if out, ok := rewriters[idx](in, r); ok {
			return out, true
		}
	}
	return in, false
}

func rewriteLiteral(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return mutateStatementExpr(stmt, rng, literalExprMutator)
	})
}

func rewriteComparison(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return mutateStatementExpr(stmt, rng, comparisonExprMutator)
	})
}

func rewriteIsNull(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return mutateStatementExpr(stmt, rng, isNullExprMutator)
	})
}

func rewriteUnion(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, _ *random.RNG) bool {
		return visitSelectFromStatement(stmt, func(sel *sqlparser.SelectStmt) bool {
			if strings.EqualFold(sel.SetOp, "union") {
				sel.SetAll = !sel.SetAll
				return true
			}
			return false
		})
	})
}

func rewriteJoinType(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return visitSelectFromStatement(stmt, func(sel *sqlparser.SelectStmt) bool {
			return mutateJoinTypeInTable(sel.From, rng)
		})
	})
}

func rewriteLimit(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return visitSelectFromStatement(stmt, func(sel *sqlparser.SelectStmt) bool {
			if sel.Limit == nil || len(sel.Limit.SLimit.Bytes) > 0 {
				return false
			}
			sel.Limit.Limit = sqlTok(strconv.Itoa(1 + rng.Intn(128)))
			sel.Limit.Offset = sqlparser.Token{}
			return true
		})
	})
}

func rewriteFillMode(in string, r *random.RNG) (string, bool) {
	return rewriteViaAST(in, r, func(stmt sqlparser.Statement, rng *random.RNG) bool {
		return visitSelectFromStatement(stmt, func(sel *sqlparser.SelectStmt) bool {
			mode := pickFillMode(rng)
			if sel.Window.Fill != nil {
				sel.Window.Fill = &sqlparser.FillExpr{Name: mode}
				return true
			}
			if sel.InterpFill != nil {
				sel.InterpFill = &sqlparser.FillExpr{Name: mode}
				return true
			}
			return false
		})
	})
}

func rewriteViaAST(in string, r *random.RNG, apply func(sqlparser.Statement, *random.RNG) bool) (string, bool) {
	stmt, ok := parseStatementForMutation(in)
	if !ok {
		return in, false
	}
	if !apply(stmt, r) {
		return in, false
	}
	out := strings.TrimSpace(sqlparser.SQLNodeToString(stmt))
	if out == "" || out == strings.TrimSpace(in) {
		return in, false
	}
	return out, true
}

func parseStatementForMutation(in string) (sqlparser.Statement, bool) {
	stmt, err := sqlparser.Parse(strings.TrimSpace(in))
	if err != nil {
		return nil, false
	}
	return stmt, true
}

type exprMutator func(sqlparser.Expr, *random.RNG) (sqlparser.Expr, bool)

func mutateStatementExpr(stmt sqlparser.Statement, r *random.RNG, mut exprMutator) bool {
	switch s := stmt.(type) {
	case *sqlparser.SelectStmt:
		return mutateSelectExpr(s, r, mut)
	case *sqlparser.InsertQueryStmt:
		if s.Query == nil {
			return false
		}
		return mutateSelectExpr(s.Query, r, mut)
	default:
		return false
	}
}

func mutateSelectExpr(sel *sqlparser.SelectStmt, r *random.RNG, mut exprMutator) bool {
	if sel == nil {
		return false
	}
	for i := range sel.Select {
		if out, ok := applyExprMutator(sel.Select[i], r, mut); ok {
			sel.Select[i] = out
			return true
		}
	}
	if mutateTableExpr(sel.From, r, mut) {
		return true
	}
	if out, ok := applyExprMutator(sel.Where, r, mut); ok {
		sel.Where = out
		return true
	}
	if out, ok := applyExprMutator(sel.Partition, r, mut); ok {
		sel.Partition = out
		return true
	}
	if out, ok := applyExprMutator(sel.Range, r, mut); ok {
		sel.Range = out
		return true
	}
	if out, ok := applyExprMutator(sel.Having, r, mut); ok {
		sel.Having = out
		return true
	}
	if sel.GroupBy != nil {
		for i := range sel.GroupBy.Exprs {
			if out, ok := applyExprMutator(sel.GroupBy.Exprs[i], r, mut); ok {
				sel.GroupBy.Exprs[i] = out
				return true
			}
		}
	}
	for i := range sel.OrderBy {
		if out, ok := applyExprMutator(sel.OrderBy[i].Expr, r, mut); ok {
			sel.OrderBy[i].Expr = out
			return true
		}
	}
	if out, ok := applyExprMutator(sel.Window.Session, r, mut); ok {
		sel.Window.Session = out
		return true
	}
	if out, ok := applyExprMutator(sel.Window.StateWindow, r, mut); ok {
		sel.Window.StateWindow = out
		return true
	}
	if out, ok := applyExprMutator(sel.Window.EventWindowStart, r, mut); ok {
		sel.Window.EventWindowStart = out
		return true
	}
	if out, ok := applyExprMutator(sel.Window.EventWindowEnd, r, mut); ok {
		sel.Window.EventWindowEnd = out
		return true
	}
	if out, ok := applyExprMutator(sel.Window.AnomalyWindow, r, mut); ok {
		sel.Window.AnomalyWindow = out
		return true
	}
	if sel.Window.Fill != nil {
		for i := range sel.Window.Fill.Values {
			if out, ok := applyExprMutator(sel.Window.Fill.Values[i], r, mut); ok {
				sel.Window.Fill.Values[i] = out
				return true
			}
		}
	}
	if sel.InterpFill != nil {
		for i := range sel.InterpFill.Values {
			if out, ok := applyExprMutator(sel.InterpFill.Values[i], r, mut); ok {
				sel.InterpFill.Values[i] = out
				return true
			}
		}
	}
	if mutateSelectExpr(sel.Left, r, mut) {
		return true
	}
	if mutateSelectExpr(sel.Right, r, mut) {
		return true
	}
	return false
}

func mutateTableExpr(t sqlparser.TableExpr, r *random.RNG, mut exprMutator) bool {
	switch n := t.(type) {
	case *sqlparser.SubqueryTableExpr:
		if n.Query == nil {
			return false
		}
		return mutateSelectExpr(n.Query, r, mut)
	case *sqlparser.JoinTableExpr:
		if mutateTableExpr(n.Left, r, mut) {
			return true
		}
		if mutateTableExpr(n.Right, r, mut) {
			return true
		}
		if cond, ok := n.Condition.(sqlparser.Expr); ok {
			if out, changed := applyExprMutator(cond, r, mut); changed {
				n.Condition = out
				return true
			}
		}
		if out, ok := applyExprMutator(n.WindowOffset, r, mut); ok {
			n.WindowOffset = out
			return true
		}
	}
	return false
}

func applyExprMutator(expr sqlparser.Expr, r *random.RNG, mut exprMutator) (sqlparser.Expr, bool) {
	if expr == nil {
		return expr, false
	}
	if out, ok := mut(expr, r); ok {
		return out, true
	}

	switch n := expr.(type) {
	case *sqlparser.AliasedExpr:
		if n.Expr == nil {
			return expr, false
		}
		if out, ok := applyExprMutator(n.Expr, r, mut); ok {
			n.Expr = out
			return n, true
		}
	case *sqlparser.RawExpr:
		if out, ok := applyExprMutator(n.Left, r, mut); ok {
			n.Left = out
			return n, true
		}
		if out, ok := applyExprMutator(n.Right, r, mut); ok {
			n.Right = out
			return n, true
		}
		for i := range n.Args {
			if out, ok := applyExprMutator(n.Args[i], r, mut); ok {
				n.Args[i] = out
				return n, true
			}
		}
		if ex, ok := n.Extra.(sqlparser.Expr); ok {
			if out, changed := applyExprMutator(ex, r, mut); changed {
				n.Extra = out
				return n, true
			}
		}
		if sub, ok := n.Extra.(*sqlparser.SelectStmt); ok {
			if mutateSelectExpr(sub, r, mut) {
				return n, true
			}
		}
	case *sqlparser.SelectStmt:
		if mutateSelectExpr(n, r, mut) {
			return n, true
		}
	}
	return expr, false
}

func literalExprMutator(expr sqlparser.Expr, r *random.RNG) (sqlparser.Expr, bool) {
	switch n := expr.(type) {
	case sqlparser.Literal:
		switch n.Type {
		case sqlparser.LiteralInt:
			n.Val = sqlTok(strconv.Itoa(1 + r.Intn(200)))
			return n, true
		case sqlparser.LiteralFloat:
			n.Val = sqlTok(fmt.Sprintf("%d.%d", 1+r.Intn(20), r.Intn(99)))
			return n, true
		case sqlparser.LiteralString:
			n.Val = sqlTok(fmt.Sprintf("x_%d", r.Intn(9999)))
			return n, true
		case sqlparser.LiteralBool:
			cur := strings.ToLower(string(n.Val.Bytes))
			if cur == "true" {
				n.Val = sqlTok("false")
			} else {
				n.Val = sqlTok("true")
			}
			return n, true
		case sqlparser.LiteralDuration:
			n.Val = sqlTok(pickDurationLiteral(r))
			return n, true
		default:
			return expr, false
		}
	case *sqlparser.SQLVal:
		switch n.Type {
		case sqlparser.IntVal:
			n.Val = []byte(strconv.Itoa(1 + r.Intn(200)))
			return n, true
		case sqlparser.FloatVal:
			n.Val = []byte(fmt.Sprintf("%d.%d", 1+r.Intn(20), r.Intn(99)))
			return n, true
		case sqlparser.StrVal:
			n.Val = []byte(fmt.Sprintf("x_%d", r.Intn(9999)))
			return n, true
		case sqlparser.DurationVal:
			n.Val = []byte(pickDurationLiteral(r))
			return n, true
		default:
			return expr, false
		}
	case sqlparser.BoolVal:
		return sqlparser.BoolVal(!bool(n)), true
	default:
		return expr, false
	}
}

func comparisonExprMutator(expr sqlparser.Expr, r *random.RNG) (sqlparser.Expr, bool) {
	raw, ok := expr.(*sqlparser.RawExpr)
	if !ok || raw == nil || raw.Kind != "cmp" {
		return expr, false
	}
	if repl, ok := alternateComparisonOp(raw.Op, r); ok {
		raw.Op = repl
		return raw, true
	}
	return expr, false
}

func isNullExprMutator(expr sqlparser.Expr, _ *random.RNG) (sqlparser.Expr, bool) {
	raw, ok := expr.(*sqlparser.RawExpr)
	if !ok || raw == nil || raw.Kind != "is_null" {
		return expr, false
	}
	not := false
	if cur, ok := raw.Extra.(bool); ok {
		not = cur
	}
	raw.Extra = !not
	return raw, true
}

func alternateComparisonOp(op sqlparser.Token, r *random.RNG) (sqlparser.Token, bool) {
	switch string(op.Bytes) {
	case "gt":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_LOWER_THAN, sqlparser.OP_TYPE_GREATER_EQUAL, sqlparser.OP_TYPE_EQUAL}), true
	case "lt":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_GREATER_THAN, sqlparser.OP_TYPE_LOWER_EQUAL, sqlparser.OP_TYPE_EQUAL}), true
	case "ge":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_LOWER_EQUAL, sqlparser.OP_TYPE_GREATER_THAN, sqlparser.OP_TYPE_EQUAL}), true
	case "le":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_GREATER_EQUAL, sqlparser.OP_TYPE_LOWER_THAN, sqlparser.OP_TYPE_EQUAL}), true
	case "eq":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_NOT_EQUAL, sqlparser.OP_TYPE_GREATER_THAN, sqlparser.OP_TYPE_LOWER_THAN}), true
	case "ne":
		return pickToken(r, []sqlparser.Token{sqlparser.OP_TYPE_EQUAL, sqlparser.OP_TYPE_GREATER_THAN, sqlparser.OP_TYPE_LOWER_THAN}), true
	case "like":
		return sqlparser.OP_TYPE_NOT_LIKE, true
	case "not_like":
		return sqlparser.OP_TYPE_LIKE, true
	default:
		return sqlparser.Token{}, false
	}
}

func visitSelectFromStatement(stmt sqlparser.Statement, visit func(*sqlparser.SelectStmt) bool) bool {
	switch s := stmt.(type) {
	case *sqlparser.SelectStmt:
		return visitSelect(s, visit)
	case *sqlparser.InsertQueryStmt:
		if s.Query == nil {
			return false
		}
		return visitSelect(s.Query, visit)
	default:
		return false
	}
}

func visitSelect(sel *sqlparser.SelectStmt, visit func(*sqlparser.SelectStmt) bool) bool {
	if sel == nil {
		return false
	}
	if visit(sel) {
		return true
	}
	if visitSelect(sel.Left, visit) {
		return true
	}
	if visitSelect(sel.Right, visit) {
		return true
	}
	for _, e := range sel.Select {
		if visitSelectInExpr(e, visit) {
			return true
		}
	}
	if visitSelectInTable(sel.From, visit) {
		return true
	}
	if visitSelectInExpr(sel.Where, visit) {
		return true
	}
	if visitSelectInExpr(sel.Partition, visit) {
		return true
	}
	if visitSelectInExpr(sel.Range, visit) {
		return true
	}
	if visitSelectInExpr(sel.Having, visit) {
		return true
	}
	if sel.GroupBy != nil {
		for _, e := range sel.GroupBy.Exprs {
			if visitSelectInExpr(e, visit) {
				return true
			}
		}
	}
	for _, ob := range sel.OrderBy {
		if visitSelectInExpr(ob.Expr, visit) {
			return true
		}
	}
	if visitSelectInExpr(sel.Window.Session, visit) {
		return true
	}
	if visitSelectInExpr(sel.Window.StateWindow, visit) {
		return true
	}
	if visitSelectInExpr(sel.Window.EventWindowStart, visit) {
		return true
	}
	if visitSelectInExpr(sel.Window.EventWindowEnd, visit) {
		return true
	}
	if visitSelectInExpr(sel.Window.AnomalyWindow, visit) {
		return true
	}
	if sel.Window.Fill != nil {
		for _, e := range sel.Window.Fill.Values {
			if visitSelectInExpr(e, visit) {
				return true
			}
		}
	}
	if sel.InterpFill != nil {
		for _, e := range sel.InterpFill.Values {
			if visitSelectInExpr(e, visit) {
				return true
			}
		}
	}
	return false
}

func visitSelectInTable(t sqlparser.TableExpr, visit func(*sqlparser.SelectStmt) bool) bool {
	switch n := t.(type) {
	case *sqlparser.SubqueryTableExpr:
		if n.Query == nil {
			return false
		}
		return visitSelect(n.Query, visit)
	case *sqlparser.JoinTableExpr:
		if visitSelectInTable(n.Left, visit) {
			return true
		}
		if visitSelectInTable(n.Right, visit) {
			return true
		}
		if cond, ok := n.Condition.(sqlparser.Expr); ok {
			if visitSelectInExpr(cond, visit) {
				return true
			}
		}
		if visitSelectInExpr(n.WindowOffset, visit) {
			return true
		}
	}
	return false
}

func visitSelectInExpr(expr sqlparser.Expr, visit func(*sqlparser.SelectStmt) bool) bool {
	switch n := expr.(type) {
	case *sqlparser.SelectStmt:
		return visitSelect(n, visit)
	case *sqlparser.AliasedExpr:
		if n == nil {
			return false
		}
		return visitSelectInExpr(n.Expr, visit)
	case *sqlparser.RawExpr:
		if n == nil {
			return false
		}
		if visitSelectInExpr(n.Left, visit) {
			return true
		}
		if visitSelectInExpr(n.Right, visit) {
			return true
		}
		for _, arg := range n.Args {
			if visitSelectInExpr(arg, visit) {
				return true
			}
		}
		if sub, ok := n.Extra.(*sqlparser.SelectStmt); ok {
			return visitSelect(sub, visit)
		}
		if ex, ok := n.Extra.(sqlparser.Expr); ok {
			return visitSelectInExpr(ex, visit)
		}
	}
	return false
}

func mutateJoinTypeInTable(t sqlparser.TableExpr, r *random.RNG) bool {
	switch n := t.(type) {
	case *sqlparser.SubqueryTableExpr:
		if n.Query == nil {
			return false
		}
		return visitSelect(n.Query, func(sel *sqlparser.SelectStmt) bool {
			return mutateJoinTypeInTable(sel.From, r)
		})
	case *sqlparser.JoinTableExpr:
		if next, ok := alternateJoinType(n.JoinType, r); ok {
			n.JoinType = next
			return true
		}
		if mutateJoinTypeInTable(n.Left, r) {
			return true
		}
		if mutateJoinTypeInTable(n.Right, r) {
			return true
		}
	}
	return false
}

func alternateJoinType(in sqlparser.JoinType, r *random.RNG) (sqlparser.JoinType, bool) {
	switch in {
	case sqlparser.JoinTypeLeft:
		return sqlparser.JoinTypeRight, true
	case sqlparser.JoinTypeRight:
		return sqlparser.JoinTypeLeft, true
	case sqlparser.JoinTypeFull:
		return sqlparser.JoinTypeInner, true
	case sqlparser.JoinTypeInner:
		if r.Intn(100) < 50 {
			return sqlparser.JoinTypeLeft, true
		}
		return sqlparser.JoinTypeRight, true
	default:
		return 0, false
	}
}

func pickToken(r *random.RNG, arr []sqlparser.Token) sqlparser.Token {
	if len(arr) == 0 {
		return sqlparser.Token{}
	}
	return arr[r.Intn(len(arr))]
}

func pickFillMode(r *random.RNG) string {
	modes := []string{"prev", "next", "near", "null"}
	return modes[r.Intn(len(modes))]
}

func pickDurationLiteral(r *random.RNG) string {
	items := []string{"1s", "5s", "10s", "30s", "1m"}
	return items[r.Intn(len(items))]
}

func sqlTok(s string) sqlparser.Token {
	return sqlparser.Token{Bytes: []byte(s)}
}
