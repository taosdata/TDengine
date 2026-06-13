package querygen

import (
	"fmt"
	"strconv"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/random"
)

func (g *Generator) querySpecificationASTPure(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	ctx.add("query_specification")
	if chance(r, 35) {
		ctx.add("safe_simple")
		return g.safeSimpleQuerySpecAST(r, ctx)
	}

	enableFrom := true
	enableWhere := chance(r, 56)
	enablePartition := chance(r, 22)
	enableRange := chance(r, 16)
	enableEvery := chance(r, 18)
	enableInterpFill := chance(r, 20)
	enableWindow := chance(r, 30)
	enableGroup := chance(r, 35)

	stmt := &sqlparser.SelectStmt{}
	if chance(r, 25) {
		ctx.add("hint")
		stmt.Hint = g.hintOptionAST(r, enableFrom, enableWindow)
	}
	if chance(r, 22) {
		stmt.IsDistinct = chance(r, 55)
	}
	if chance(r, 12) {
		ctx.add("tag_mode")
		stmt.TagScan = true
	}

	var groupedExprs []sqlparser.Expr
	if enableGroup {
		groupedExprs = g.groupByExprListAST(r, ctx)
		stmt.Select = g.groupedSelectListAST(r, depth, ctx, groupedExprs)
	} else {
		stmt.Select = g.selectListAST(r, depth, ctx)
	}

	if enableFrom {
		stmt.From = g.fromClauseAST(r, depth, ctx)
	}
	if enableWhere {
		stmt.Where = g.searchConditionAST(r, depth, ctx)
	}
	if enablePartition {
		stmt.Partition = g.partitionClauseAST(r, depth, ctx)
	}

	windowKind := ""
	if enableWindow {
		stmt.Window, windowKind = g.twindowClauseAST(r, depth, ctx)
		if !windowExprIsEmpty(stmt.Window) {
			enableGroup = false
		}
	}
	_ = windowKind
	if enableRange {
		stmt.Range = g.rangeClauseAST(r, depth, ctx)
	}
	if enableEvery {
		stmt.Every = g.everyClauseAST(r, ctx)
	}
	if enableInterpFill {
		stmt.InterpFill = g.interpFillClauseAST(r, depth, ctx)
	}

	if enableGroup {
		stmt.GroupBy = &sqlparser.GroupByExpr{Exprs: groupedExprs}
		if chance(r, 50) {
			stmt.Having = g.groupedHavingClauseAST(r, ctx)
		}
	} else if chance(r, 10) {
		stmt.Having = g.havingClauseAST(r, depth, ctx)
	}
	return stmt
}

func (g *Generator) safeSimpleQuerySpecAST(r *random.RNG, ctx *genCtx) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	if table == "" {
		table = "t1"
	}

	items := make([]sqlparser.Expr, 0, 3)
	n := 1 + r.Intn(3)
	for i := 0; i < n; i++ {
		items = append(items, g.columnExprByName(g.columnName(r), ""))
	}

	stmt := &sqlparser.SelectStmt{
		Select: items,
		From:   &sqlparser.TableNameExpr{TableName: table},
	}
	if chance(r, 55) {
		stmt.Where = sqlparser.NewComparisonExpr(
			nil,
			g.columnExprKindAST(r, kindNumber),
			sqlparser.OP_TYPE_GREATER_THAN,
			g.literalNumberAST(r),
		)
	}
	if chance(r, 45) {
		ctx.add("order_by")
		stmt.OrderBy = []sqlparser.OrderByExpr{{
			Expr:       g.columnExprByName(g.columnName(r), ""),
			Asc:        chance(r, 50),
			NullsFirst: chance(r, 50),
		}}
	}
	if chance(r, 60) {
		stmt.Limit = g.limitAST(r, ctx)
	}
	return stmt
}

func (g *Generator) hintOptionAST(r *random.RNG, hasFrom bool, hasWindow bool) *sqlparser.HintOption {
	hints := []sqlparser.HintType{
		sqlparser.HINT_BATCH_SCAN,
		sqlparser.HINT_NO_BATCH_SCAN,
		sqlparser.HINT_SORT_FOR_GROUP,
		sqlparser.HINT_PARTITION_FIRST,
		sqlparser.HINT_SKIP_TSMA,
	}
	if hasFrom {
		hints = append(hints, sqlparser.HINT_HASH_JOIN)
	}
	if hasWindow {
		if chance(r, 50) {
			hints = append(hints, sqlparser.HINT_WIN_OPTIMIZE_BATCH)
		} else {
			hints = append(hints, sqlparser.HINT_WIN_OPTIMIZE_SINGLE)
		}
	}
	if len(hints) == 0 {
		return nil
	}
	return sqlparser.NewHintOption(hints[r.Intn(len(hints))])
}

func (g *Generator) selectListAST(r *random.RNG, depth int, ctx *genCtx) []sqlparser.Expr {
	ctx.add("select_list")
	n := 1 + r.Intn(g.cfg.MaxSelectItems)
	items := make([]sqlparser.Expr, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, g.selectItemAST(r, depth, ctx))
	}
	return items
}

func (g *Generator) selectItemAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("select_item")
	switch r.Intn(8) {
	case 0, 1:
		return &sqlparser.StarExpr{}
	default:
		expr := g.commonExpressionAST(r, depth, ctx)
		if chance(r, 38) {
			return &sqlparser.AliasedExpr{Expr: expr, Alias: g.alias(r)}
		}
		return expr
	}
}

func (g *Generator) fromClauseAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.TableExpr {
	ctx.add("from_clause")
	return g.tableReferenceAST(r, depth, ctx)
}

func (g *Generator) tableReferenceAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.TableExpr {
	ctx.add("table_reference")
	if depth > 0 && chance(r, 10) {
		ctx.add("join")
		return g.joinedTableAST(r, depth-1, ctx)
	}
	return g.tablePrimaryAST(r, depth, ctx)
}

func (g *Generator) tablePrimaryAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.TableExpr {
	ctx.add("table_primary")
	switch {
	case depth > 0 && chance(r, 24):
		ctx.add("subquery")
		return &sqlparser.SubqueryTableExpr{
			Query: g.queryExpressionAST(r, depth-1, ctx),
			Alias: g.alias(r),
		}
	case depth > 0 && chance(r, 10):
		ctx.add("parenthesized_joined_table")
		return &sqlparser.ParenthesizedTableExpr{Inner: g.joinedTableAST(r, depth-1, ctx)}
	default:
		t := g.tableRefName(r)
		if t == "" {
			t = "t1"
		}
		table := &sqlparser.TableNameExpr{TableName: t}
		if chance(r, 12) {
			table.DBName = "db1"
			ctx.add("db_name")
		}
		if chance(r, 60) {
			table.Alias = g.alias(r)
		}
		return table
	}
}

func (g *Generator) joinedTableAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.TableExpr {
	lt, rt := g.pickTwoTableNames(r)
	left := sqlparser.TableExpr(&sqlparser.TableNameExpr{TableName: lt})
	right := sqlparser.TableExpr(&sqlparser.TableNameExpr{TableName: rt})
	if depth > 0 && chance(r, 20) {
		left = g.tablePrimaryAST(r, depth-1, ctx)
	}
	if depth > 0 && chance(r, 20) {
		right = g.tablePrimaryAST(r, depth-1, ctx)
	}

	join := &sqlparser.JoinTableExpr{
		Left:     left,
		Right:    right,
		JoinType: sqlparser.JoinTypeInner,
		Condition: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(g.timeColumnForTable(lt), lt),
			sqlparser.OP_TYPE_EQUAL,
			g.columnExprByName(g.timeColumnForTable(rt), rt),
		),
	}

	switch r.Intn(12) {
	case 0, 1:
		join.JoinType = sqlparser.JoinTypeInner
	case 2:
		join.JoinType = sqlparser.JoinTypeLeft
	case 3:
		join.JoinType = sqlparser.JoinTypeRight
	case 4:
		join.JoinType = sqlparser.JoinTypeFull
	case 5:
		join.JoinType = sqlparser.JoinTypeLeft
	case 6:
		join.JoinType = sqlparser.JoinTypeRight
	case 7:
		join.JoinType = sqlparser.JoinTypeLeftSemi
	case 8:
		join.JoinType = sqlparser.JoinTypeRightAnti
	case 9:
		join.JoinType = sqlparser.JoinTypeLeftAsof
		if chance(r, 35) {
			join.JLimit = &sqlparser.LimitExpr{Limit: g.unsignedIntToken(r)}
		}
	case 10:
		join.JoinType = sqlparser.JoinTypeRightAsof
		if chance(r, 35) {
			join.JLimit = &sqlparser.LimitExpr{Limit: g.unsignedIntToken(r)}
		}
	default:
		if chance(r, 50) {
			join.JoinType = sqlparser.JoinTypeLeftWindow
		} else {
			join.JoinType = sqlparser.JoinTypeRightWindow
		}
		join.WindowOffset = sqlparser.NewWindowOffsetExpr(
			g.durationExprAST(pick(r, g.durationLit)),
			sqlparser.NewUnaryExpr(nil, sqlparser.OP_TYPE_MINUS, g.durationExprAST(pick(r, g.durationLit))),
		)
		if chance(r, 35) {
			join.JLimit = &sqlparser.LimitExpr{Limit: g.unsignedIntToken(r)}
		}
	}
	return join
}

func (g *Generator) groupByExprListAST(r *random.RNG, ctx *genCtx) []sqlparser.Expr {
	ctx.add("group_by")
	n := 1 + r.Intn(2)
	exprs := make([]sqlparser.Expr, 0, n)
	seen := map[string]struct{}{}
	for len(exprs) < n {
		name := g.columnRefKind(r, kindAny)
		if name == "" {
			name = "id"
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		exprs = append(exprs, g.columnExprByName(name, ""))
	}
	return exprs
}

func (g *Generator) groupedSelectListAST(r *random.RNG, depth int, ctx *genCtx, groupedExprs []sqlparser.Expr) []sqlparser.Expr {
	ctx.add("select_list")
	ctx.add("select_item")

	items := make([]sqlparser.Expr, 0, len(groupedExprs)+2)
	for _, e := range groupedExprs {
		if chance(r, 35) {
			items = append(items, &sqlparser.AliasedExpr{Expr: e, Alias: g.alias(r)})
		} else {
			items = append(items, e)
		}
	}

	aggCount := 1 + r.Intn(2)
	for i := 0; i < aggCount; i++ {
		switch r.Intn(4) {
		case 0:
			items = append(items, sqlparser.NewFuncExpr(nil, "count", []sqlparser.Expr{&sqlparser.StarExpr{}}))
		case 1:
			items = append(items, sqlparser.NewFuncExpr(nil, "sum", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}))
		case 2:
			items = append(items, sqlparser.NewFuncExpr(nil, "max", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}))
		default:
			items = append(items, sqlparser.NewFuncExpr(nil, "avg", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}))
		}
	}
	return items
}

func (g *Generator) groupedHavingClauseAST(r *random.RNG, ctx *genCtx) sqlparser.Expr {
	ctx.add("having")
	switch r.Intn(3) {
	case 0:
		return sqlparser.NewComparisonExpr(
			nil,
			sqlparser.NewFuncExpr(nil, "count", []sqlparser.Expr{&sqlparser.StarExpr{}}),
			sqlparser.OP_TYPE_GREATER_THAN,
			g.literalNumberAST(r),
		)
	case 1:
		return sqlparser.NewComparisonExpr(
			nil,
			sqlparser.NewFuncExpr(nil, "sum", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}),
			sqlparser.OP_TYPE_GREATER_THAN,
			g.literalNumberAST(r),
		)
	default:
		return sqlparser.NewIsNullExpr(
			nil,
			sqlparser.NewFuncExpr(nil, "max", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}),
			true,
		)
	}
}

func (g *Generator) havingClauseAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("having")
	return g.searchConditionAST(r, depth, ctx)
}

func (g *Generator) partitionClauseAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("partition")
	n := 1 + r.Intn(2)
	items := make([]sqlparser.Expr, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, g.expressionAST(r, depth, ctx))
	}
	return sqlparser.NewPartitionByExpr(nil, items)
}

func (g *Generator) rangeClauseAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("range")
	n := 1 + r.Intn(2)
	exprs := make([]sqlparser.Expr, 0, n)
	for i := 0; i < n; i++ {
		exprs = append(exprs, g.expressionKindAST(r, depth, ctx, kindTime))
	}
	switch len(exprs) {
	case 1:
		return &sqlparser.RawExpr{Kind: "range_1", Args: exprs}
	default:
		return &sqlparser.RawExpr{Kind: "range_2", Args: exprs[:2]}
	}
}

func (g *Generator) everyClauseAST(r *random.RNG, ctx *genCtx) sqlparser.Literal {
	ctx.add("every")
	return sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}
}

func (g *Generator) interpFillClauseAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.FillExpr {
	ctx.add("fill")
	exprs := g.expressionListAST(r, min(depth, 1), ctx)
	switch r.Intn(7) {
	case 0:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_NEAR, Values: exprs}
	case 1:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_PREV, Values: exprs}
	case 2:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_NEXT, Values: exprs}
	case 3:
		return &sqlparser.FillExpr{Name: "near"}
	case 4:
		return &sqlparser.FillExpr{Name: "prev"}
	case 5:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_VALUE, Values: exprs}
	default:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_VALUE_F, Values: exprs}
	}
}

func (g *Generator) twindowClauseAST(r *random.RNG, depth int, ctx *genCtx) (sqlparser.WindowExpr, string) {
	ctx.add("window")
	switch r.Intn(7) {
	case 0:
		w := sqlparser.WindowExpr{Interval: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}}
		if chance(r, 45) {
			w.Fill = g.fillOptAST(r, depth, ctx)
		}
		if chance(r, 35) {
			w.Sliding = sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}
		}
		return w, "interval"
	case 1:
		w := sqlparser.WindowExpr{
			Interval: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
			Offset:   sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
		}
		if chance(r, 45) {
			w.Fill = g.fillOptAST(r, depth, ctx)
		}
		if chance(r, 35) {
			w.Sliding = sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}
		}
		return w, "interval"
	case 2:
		return sqlparser.WindowExpr{
			Session:    g.columnExprKindAST(r, kindTime),
			SessionGap: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
		}, "session"
	case 3:
		w := sqlparser.WindowExpr{StateWindow: g.columnExprKindAST(r, kindNumber)}
		if chance(r, 65) {
			w.StateWindowOpt.HasExtend = true
			w.StateWindowOpt.Extend = sqlparser.Literal{Val: sqlTok(strconv.Itoa(1 + r.Intn(3))), Type: sqlparser.LiteralInt}
		}
		if chance(r, 50) {
			w.StateWindowOpt.HasExtend = true
			w.StateWindowOpt.HasZeroth = true
			if chance(r, 50) {
				w.StateWindowOpt.Zeroth = sqlparser.Literal{Val: sqlTok(fmt.Sprintf("s_%d", r.Intn(999))), Type: sqlparser.LiteralString}
			} else {
				v := 1 + r.Intn(3)
				if chance(r, 50) {
					w.StateWindowOpt.Zeroth = sqlparser.Literal{Val: sqlTok("-" + strconv.Itoa(v)), Type: sqlparser.LiteralInt}
				} else {
					w.StateWindowOpt.Zeroth = sqlparser.Literal{Val: sqlTok(strconv.Itoa(v)), Type: sqlparser.LiteralInt}
				}
			}
		}
		if chance(r, 35) {
			w.TrueFor = sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}
		}
		return w, "state"
	case 4:
		w := sqlparser.WindowExpr{
			CountWindow:      g.unsignedIntToken(r),
			CountWindowSlide: g.unsignedIntToken(r),
		}
		colN := 2 + r.Intn(2)
		w.CountWindowCols = make([]sqlparser.ColumnExpr, 0, colN)
		for i := 0; i < colN; i++ {
			if chance(r, 20) {
				w.CountWindowCols = append(w.CountWindowCols, sqlparser.ColumnExpr("tbname"))
				continue
			}
			name := strings.TrimSpace(g.columnRefKind(r, kindAny))
			if name == "" {
				name = "id"
			}
			w.CountWindowCols = append(w.CountWindowCols, sqlparser.ColumnExpr(name))
		}
		return w, "count"
	case 5:
		w := sqlparser.WindowExpr{
			EventWindowStart: g.searchConditionAST(r, min(depth, 1), ctx),
			EventWindowEnd:   g.searchConditionAST(r, min(depth, 1), ctx),
		}
		if chance(r, 35) {
			w.TrueFor = sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration}
		}
		return w, "event"
	default:
		w := sqlparser.WindowExpr{AnomalyWindow: g.columnExprKindAST(r, kindNumber)}
		if chance(r, 45) {
			w.AnomalyTag = sqlTok("strict")
		}
		return w, "anomaly"
	}
}

func (g *Generator) fillOptAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.FillExpr {
	ctx.add("fill")
	switch r.Intn(3) {
	case 0:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_VALUE, Values: g.expressionListAST(r, min(depth, 1), ctx)}
	case 1:
		return &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_VALUE_F, Values: g.expressionListAST(r, min(depth, 1), ctx)}
	default:
		return &sqlparser.FillExpr{Name: pick(r, []string{"none", "null", "null_f", "linear", "prev", "next"})}
	}
}

func (g *Generator) orderByAST(r *random.RNG, depth int, ctx *genCtx) []sqlparser.OrderByExpr {
	ctx.add("order_by")
	n := 1 + r.Intn(2)
	items := make([]sqlparser.OrderByExpr, 0, n)
	for i := 0; i < n; i++ {
		asc := chance(r, 50)
		if chance(r, 60) {
			asc = chance(r, 50)
		}
		nullsFirst := chance(r, 50)
		if !chance(r, 30) {
			nullsFirst = false
		}
		items = append(items, sqlparser.OrderByExpr{
			Expr:       g.expressionAST(r, depth, ctx),
			Asc:        asc,
			NullsFirst: nullsFirst,
		})
	}
	return items
}

func (g *Generator) limitAST(r *random.RNG, ctx *genCtx) *sqlparser.LimitExpr {
	ctx.add("limit")
	l := g.unsignedIntToken(r)
	o := g.unsignedIntToken(r)
	if r.Intn(3) == 0 {
		return &sqlparser.LimitExpr{Limit: l}
	}
	return &sqlparser.LimitExpr{Limit: l, Offset: o}
}

func (g *Generator) slimitAST(r *random.RNG, ctx *genCtx) *sqlparser.LimitExpr {
	ctx.add("slimit")
	l := g.unsignedIntToken(r)
	o := g.unsignedIntToken(r)
	if r.Intn(3) == 0 {
		return &sqlparser.LimitExpr{SLimit: l}
	}
	return &sqlparser.LimitExpr{SLimit: l, SOffset: o}
}

func (g *Generator) searchConditionAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("search_condition")
	if depth <= 0 {
		if chance(r, 40) {
			return g.commonExpressionAST(r, 0, ctx)
		}
		return g.booleanPrimaryAST(r, 0, ctx)
	}
	switch r.Intn(5) {
	case 0:
		return sqlparser.NewUnaryExpr(nil, sqlparser.LOGIC_COND_TYPE_NOT, g.booleanPrimaryAST(r, depth-1, ctx))
	case 1:
		return sqlparser.NewBinaryExpr(
			nil,
			g.searchConditionAST(r, depth-1, ctx),
			sqlparser.LOGIC_COND_TYPE_AND,
			g.searchConditionAST(r, depth-1, ctx),
		)
	case 2:
		return sqlparser.NewBinaryExpr(
			nil,
			g.searchConditionAST(r, depth-1, ctx),
			sqlparser.LOGIC_COND_TYPE_OR,
			g.searchConditionAST(r, depth-1, ctx),
		)
	default:
		if chance(r, 35) {
			return g.commonExpressionAST(r, depth-1, ctx)
		}
		return g.booleanPrimaryAST(r, depth-1, ctx)
	}
}

func (g *Generator) booleanPrimaryAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	if depth > 0 && chance(r, 20) {
		return g.searchConditionAST(r, depth-1, ctx)
	}
	return g.predicateAST(r, depth, ctx)
}

func (g *Generator) predicateAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	ctx.add("predicate")
	kind := g.pickPredicateKind(r)
	left := g.expressionKindAST(r, depth, ctx, kind)
	right := g.expressionKindAST(r, depth, ctx, kind)
	switch r.Intn(8) {
	case 0:
		ops := []sqlparser.Token{
			sqlparser.OP_TYPE_LOWER_THAN,
			sqlparser.OP_TYPE_GREATER_THAN,
			sqlparser.OP_TYPE_LOWER_EQUAL,
			sqlparser.OP_TYPE_GREATER_EQUAL,
			sqlparser.OP_TYPE_NOT_EQUAL,
			sqlparser.OP_TYPE_EQUAL,
			sqlparser.OP_TYPE_LIKE,
			sqlparser.OP_TYPE_NOT_LIKE,
			sqlparser.OP_TYPE_MATCH,
			sqlparser.OP_TYPE_NMATCH,
			sqlparser.OP_TYPE_REGEXP,
			sqlparser.OP_TYPE_NOT_REGEXP,
			sqlparser.OP_TYPE_JSON_CONTAINS,
		}
		return sqlparser.NewComparisonExpr(nil, left, ops[r.Intn(len(ops))], right)
	case 1:
		return sqlparser.NewBetweenExpr(nil, left, right, g.expressionKindAST(r, depth, ctx, kind), false)
	case 2:
		return sqlparser.NewBetweenExpr(nil, left, right, g.expressionKindAST(r, depth, ctx, kind), true)
	case 3:
		return sqlparser.NewIsNullExpr(nil, left, false)
	case 4:
		return sqlparser.NewIsNullExpr(nil, left, true)
	case 5:
		return sqlparser.NewFuncExpr(nil, "isnull", []sqlparser.Expr{left})
	case 6:
		return sqlparser.NewFuncExpr(nil, "isnotnull", []sqlparser.Expr{left})
	default:
		vals := []sqlparser.Expr{g.literalExprOfKindAST(r, kind), g.literalExprOfKindAST(r, kind), g.literalExprOfKindAST(r, kind)}
		op := sqlparser.OP_TYPE_IN
		if chance(r, 45) {
			op = sqlparser.OP_TYPE_NOT_IN
		}
		return sqlparser.NewInExpr(nil, left, op, vals)
	}
}

func (g *Generator) commonExpressionAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	if depth > 0 && chance(r, 25) {
		return g.searchConditionAST(r, depth-1, ctx)
	}
	return g.expressionKindAST(r, depth, ctx, kindAny)
}

func (g *Generator) expressionAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	return g.expressionKindAST(r, depth, ctx, kindAny)
}

func (g *Generator) expressionKindAST(r *random.RNG, depth int, ctx *genCtx, expect valueKind) sqlparser.Expr {
	if depth <= 0 {
		return g.terminalExpressionKindAST(r, ctx, expect)
	}
	switch r.Intn(12) {
	case 0:
		return g.terminalExpressionKindAST(r, ctx, expect)
	case 1:
		return g.expressionKindAST(r, depth-1, ctx, expect)
	case 2:
		return sqlparser.NewUnaryExpr(nil, sqlparser.OP_TYPE_UPLUS, g.expressionKindAST(r, depth-1, ctx, kindNumber))
	case 3:
		return sqlparser.NewUnaryExpr(nil, sqlparser.OP_TYPE_MINUS, g.expressionKindAST(r, depth-1, ctx, kindNumber))
	case 4:
		ops := []sqlparser.Token{sqlparser.OP_TYPE_ADD, sqlparser.OP_TYPE_SUB, sqlparser.OP_TYPE_MULTI, sqlparser.OP_TYPE_DIV, sqlparser.OP_TYPE_REM, sqlparser.OP_TYPE_BIT_AND, sqlparser.OP_TYPE_BIT_OR}
		return sqlparser.NewBinaryExpr(
			nil,
			g.expressionKindAST(r, depth-1, ctx, kindNumber),
			ops[r.Intn(len(ops))],
			g.expressionKindAST(r, depth-1, ctx, kindNumber),
		)
	case 5:
		return g.functionExpressionKindAST(r, depth-1, ctx, expect)
	case 6:
		return g.ifExpressionAST(r, depth-1, ctx)
	case 7:
		return g.caseWhenExpressionAST(r, depth-1, ctx)
	case 8:
		return g.terminalExpressionKindAST(r, ctx, expect)
	default:
		return g.terminalExpressionKindAST(r, ctx, expect)
	}
}

func (g *Generator) terminalExpressionKindAST(r *random.RNG, ctx *genCtx, expect valueKind) sqlparser.Expr {
	switch r.Intn(9) {
	case 0, 2:
		return g.columnExprKindAST(r, expect)
	case 1:
		ctx.add("literal")
		return g.literalExprOfKindAST(r, expect)
	case 3:
		if expect == kindAny && chance(r, 50) {
			return g.pseudoColumnExprAST(r, ctx)
		}
		return g.functionExpressionKindAST(r, 0, ctx, expect)
	case 4:
		if expect == kindAny && chance(r, 50) {
			return g.pseudoColumnExprAST(r, ctx)
		}
		return g.functionExpressionKindAST(r, 0, ctx, expect)
	default:
		return g.columnExprKindAST(r, expect)
	}
}

func (g *Generator) pseudoColumnExprAST(r *random.RNG, ctx *genCtx) sqlparser.Expr {
	ctx.add("pseudo_column")
	name := "tbname"
	if len(g.pseudoCols) > 0 {
		name = strings.ToLower(strings.TrimSpace(pick(r, g.pseudoCols)))
	}
	switch name {
	case "tbname", "rowts", "qstart", "qend", "qduration", "wstart", "wend", "wduration", "irowts", "isfilled":
	default:
		name = "tbname"
	}
	return sqlparser.NewPseudoColumnExpr(nil, name)
}

func (g *Generator) functionExpressionKindAST(r *random.RNG, depth int, ctx *genCtx, expect valueKind) sqlparser.Expr {
	ctx.add("function")
	if expect == kindNumber {
		switch r.Intn(5) {
		case 0:
			return sqlparser.NewFuncExpr(nil, "abs", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		case 1:
			return sqlparser.NewFuncExpr(nil, "ceil", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		case 2:
			return sqlparser.NewFuncExpr(nil, "floor", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		case 3:
			return sqlparser.NewFuncExpr(nil, "round", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		default:
			return sqlparser.NewFuncExpr(nil, "pow", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber), g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		}
	}
	if expect == kindString {
		switch r.Intn(4) {
		case 0:
			return sqlparser.NewFuncExpr(nil, "lower", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
		case 1:
			return sqlparser.NewFuncExpr(nil, "upper", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
		case 2:
			return sqlparser.NewFuncExpr(nil, "concat", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
		default:
			return sqlparser.NewFuncExpr(nil, "substr", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.literalNumberAST(r), g.literalNumberAST(r)})
		}
	}
	if expect == kindBool && chance(r, 50) {
		return sqlparser.NewFuncExpr(nil, "isnull", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindAny)})
	}
	if expect == kindTime && chance(r, 60) {
		return sqlparser.NewFuncExpr(nil, pick(r, []string{"now", "today"}), nil)
	}

	switch r.Intn(12) {
	case 0:
		return g.standardFunctionCallAST(r, depth, ctx)
	case 1:
		ctx.add("star_func")
		return sqlparser.NewFuncExpr(nil, "count", []sqlparser.Expr{&sqlparser.StarExpr{}})
	case 2:
		ctx.add("cols_func")
		return sqlparser.NewFuncExpr(nil, "cols", []sqlparser.Expr{
			sqlparser.NewFuncExpr(nil, "count", []sqlparser.Expr{g.columnExprKindAST(r, kindNumber)}),
			g.columnExprKindAST(r, kindAny),
		})
	case 3:
		typ := pick(r, []string{"int", "integer", "bigint", "float", "double", "bool", "timestamp", "varchar(16)", "binary(16)", "decimal(10,2)", "json", "varchar", "binary", "nchar", "varbinary"})
		return sqlparser.NewCastExpr(nil, g.commonExpressionAST(r, min(depth, 1), ctx), typ)
	case 4:
		return sqlparser.NewTrimExpr(nil, g.expressionKindAST(r, min(depth, 1), ctx, kindString), "")
	case 5:
		return sqlparser.NewTrimExprWithPattern(
			nil,
			g.expressionKindAST(r, min(depth, 1), ctx, kindString),
			g.expressionKindAST(r, min(depth, 1), ctx, kindString),
			pick(r, []string{"both", "leading", "trailing"}),
		)
	case 6:
		return sqlparser.NewTrimExprWithPattern(
			nil,
			g.expressionKindAST(r, min(depth, 1), ctx, kindString),
			g.expressionKindAST(r, min(depth, 1), ctx, kindString),
			"",
		)
	case 7:
		return sqlparser.NewPositionExpr(nil, g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.expressionKindAST(r, min(depth, 1), ctx, kindString))
	case 8:
		if chance(r, 50) {
			return sqlparser.NewFuncExpr(nil, pick(r, []string{"substr", "substring"}), []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.literalNumberAST(r), g.literalNumberAST(r)})
		}
		return sqlparser.NewFuncExpr(nil, pick(r, []string{"substr", "substring"}), []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.literalNumberAST(r)})
	case 9:
		return sqlparser.NewFuncExpr(nil, "replace", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
	case 10:
		return sqlparser.NewFuncExpr(nil, pick(r, []string{"now", "today", "timezone", "database", "client_version", "server_version", "server_status", "current_user", "user", "pi"}), nil)
	default:
		if chance(r, 50) {
			return sqlparser.NewFuncExpr(nil, "rand", nil)
		}
		return sqlparser.NewFuncExpr(nil, "rand", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
	}
}

func (g *Generator) standardFunctionCallAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	fn := pick(r, g.funcNames)
	switch fn {
	case "abs", "ceil", "floor":
		return sqlparser.NewFuncExpr(nil, fn, []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
	case "round":
		if chance(r, 70) {
			return sqlparser.NewFuncExpr(nil, "round", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
		}
		return sqlparser.NewFuncExpr(nil, "round", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber), g.literalNumberAST(r)})
	case "pow":
		return sqlparser.NewFuncExpr(nil, "pow", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber), g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
	case "length", "lower", "upper":
		return sqlparser.NewFuncExpr(nil, fn, []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
	case "concat":
		return sqlparser.NewFuncExpr(nil, "concat", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindString), g.expressionKindAST(r, min(depth, 1), ctx, kindString)})
	case "sum", "avg", "min", "max":
		return sqlparser.NewFuncExpr(nil, fn, []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
	default:
		return sqlparser.NewFuncExpr(nil, "abs", []sqlparser.Expr{g.expressionKindAST(r, min(depth, 1), ctx, kindNumber)})
	}
}

func (g *Generator) ifExpressionAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	switch r.Intn(6) {
	case 0:
		return sqlparser.NewIfExpr(nil, g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx))
	case 1:
		return sqlparser.NewIfNullExpr(nil, g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx))
	case 2:
		return sqlparser.NewFuncExpr(nil, "nvl", []sqlparser.Expr{g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx)})
	case 3:
		return sqlparser.NewFuncExpr(nil, "nvl2", []sqlparser.Expr{g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx)})
	case 4:
		return sqlparser.NewNullIfExpr(nil, g.commonExpressionAST(r, depth, ctx), g.commonExpressionAST(r, depth, ctx))
	default:
		return sqlparser.NewCoalesceExpr(nil, g.expressionListAST(r, depth, ctx))
	}
}

func (g *Generator) caseWhenExpressionAST(r *random.RNG, depth int, ctx *genCtx) sqlparser.Expr {
	whenThen := []sqlparser.WhenThenExpr{{
		When: g.commonExpressionAST(r, depth, ctx),
		Then: g.commonExpressionAST(r, depth, ctx),
	}}
	if chance(r, 50) {
		return sqlparser.NewCaseWhenExpr(nil, nil, whenThen, g.commonExpressionAST(r, depth, ctx))
	}
	return sqlparser.NewCaseWhenExpr(nil, g.commonExpressionAST(r, depth, ctx), whenThen, g.commonExpressionAST(r, depth, ctx))
}

func (g *Generator) expressionListAST(r *random.RNG, depth int, ctx *genCtx) []sqlparser.Expr {
	n := 1 + r.Intn(3)
	items := make([]sqlparser.Expr, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, g.expressionAST(r, depth, ctx))
	}
	return items
}

func (g *Generator) insertQueryAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.InsertQueryStmt {
	ctx.add("insert_query")
	dst, src := g.pickTwoTableNames(r)
	_ = depth
	insertTable := dst
	if chance(r, 20) {
		insertTable = "db1." + dst
		ctx.add("db_name")
	}

	colA := strings.TrimSpace(g.columnForTableKind(r, dst, kindAny))
	if colA == "" {
		colA = "id"
	}
	colB := strings.TrimSpace(g.columnForTableKind(r, dst, kindAny))
	if colB == "" || strings.EqualFold(colB, colA) {
		colB = "v"
	}
	cols := []string{colA, colB}
	if chance(r, 55) {
		cols = append(cols, "tbname")
	}

	selectItems := make([]sqlparser.Expr, 0, len(cols))
	for _, c := range cols {
		if strings.EqualFold(strings.TrimSpace(c), "tbname") {
			selectItems = append(selectItems, sqlparser.NewPseudoColumnExpr(nil, "tbname"))
			continue
		}
		srcCol := strings.TrimSpace(g.columnForTableKind(r, src, kindAny))
		if srcCol == "" {
			srcCol = "id"
		}
		selectItems = append(selectItems, g.columnExprByName(srcCol, ""))
	}

	return &sqlparser.InsertQueryStmt{
		Table:   insertTable,
		Columns: cols,
		Query: &sqlparser.SelectStmt{
			Select: selectItems,
			From:   &sqlparser.TableNameExpr{TableName: src},
			Limit:  &sqlparser.LimitExpr{Limit: sqlTok("1")},
		},
	}
}

func (g *Generator) columnExprKindAST(r *random.RNG, kind valueKind) sqlparser.Expr {
	name := g.columnRefKind(r, kind)
	if name == "" {
		name = "id"
	}
	return g.columnExprByName(name, "")
}

func (g *Generator) columnExprByName(name string, table string) sqlparser.Expr {
	r := &sqlparser.RawExpr{Kind: "col", Name: name}
	if table != "" {
		r.Extra = table
	}
	return r
}

func (g *Generator) literalExprOfKindAST(r *random.RNG, kind valueKind) sqlparser.Expr {
	switch kind {
	case kindBool:
		if chance(r, 50) {
			return sqlparser.BoolVal(true)
		}
		return sqlparser.BoolVal(false)
	case kindTime:
		return sqlparser.NewFuncExpr(nil, pick(r, []string{"now", "today"}), nil)
	case kindString:
		return sqlparser.Literal{Val: sqlTok(fmt.Sprintf("s_%d", r.Intn(999))), Type: sqlparser.LiteralString}
	case kindJSON:
		return sqlparser.Literal{Val: sqlTok(`{"k":1}`), Type: sqlparser.LiteralString}
	case kindNumber:
		return g.literalNumberAST(r)
	}
	if chance(r, 15) {
		return sqlparser.NewNullVal()
	}
	switch r.Intn(6) {
	case 0:
		return g.literalNumberAST(r)
	case 1:
		return sqlparser.Literal{Val: sqlTok(fmt.Sprintf("s_%d", r.Intn(999))), Type: sqlparser.LiteralString}
	case 2:
		if chance(r, 50) {
			return sqlparser.BoolVal(true)
		}
		return sqlparser.BoolVal(false)
	case 3:
		return sqlparser.NewFuncExpr(nil, pick(r, []string{"now", "today"}), nil)
	case 4:
		return sqlparser.Literal{Val: sqlTok(`{"k":1}`), Type: sqlparser.LiteralString}
	default:
		return g.literalNumberAST(r)
	}
}

func (g *Generator) literalNumberAST(r *random.RNG) sqlparser.Expr {
	if chance(r, 70) {
		return sqlparser.Literal{Val: g.unsignedIntToken(r), Type: sqlparser.LiteralInt}
	}
	v := fmt.Sprintf("%d.%d", 1+r.Intn(9), r.Intn(99))
	return sqlparser.Literal{Val: sqlTok(v), Type: sqlparser.LiteralFloat}
}

func (g *Generator) durationExprAST(text string) sqlparser.Expr {
	return sqlparser.Literal{Val: sqlTok(text), Type: sqlparser.LiteralDuration}
}

func (g *Generator) unsignedIntToken(r *random.RNG) sqlparser.Token {
	return sqlTok(strconv.Itoa(1 + r.Intn(50)))
}

func sqlTok(s string) sqlparser.Token {
	return sqlparser.Token{Bytes: []byte(s)}
}

func windowExprIsEmpty(w sqlparser.WindowExpr) bool {
	return len(w.Interval.Val.Bytes) == 0 &&
		w.Session == nil &&
		w.StateWindow == nil &&
		len(w.CountWindow.Bytes) == 0 &&
		w.EventWindowStart == nil &&
		w.EventWindowEnd == nil &&
		w.AnomalyWindow == nil
}
