package querygen

// targeted.go generates focused queries aimed at exercising specific grammar rules that
// coverage reports flag as missing, scoring templates by relevance to those rules.
//
// targeted.go 生成有针对性的查询，旨在覆盖那些被覆盖率报告标记为缺失的特定语法规则，
// 并按模板与这些规则的相关性进行打分。

import (
	"fmt"
	"sort"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

// targetedTemplate identifies a hand-written query shape aimed at a particular grammar feature.
//
// targetedTemplate 标识一种针对特定语法特性、手写的查询形态。
type targetedTemplate string

const (
	targetedJoin          targetedTemplate = "join"               // inner join with WHERE and LIMIT / 带 WHERE 和 LIMIT 的内联接
	targetedParenJoin     targetedTemplate = "parenthesized_join" // join wrapped in parentheses in FROM / FROM 中用括号包裹的联接
	targetedWindowFill    targetedTemplate = "window_fill"        // INTERVAL window with FILL / 带 FILL 的 INTERVAL 窗口
	targetedStateWindow   targetedTemplate = "state_window"       // STATE_WINDOW with extend/zeroth options / 带 extend/zeroth 选项的 STATE_WINDOW
	targetedCountWindow   targetedTemplate = "count_window"       // COUNT_WINDOW with trigger columns / 带触发列的 COUNT_WINDOW
	targetedInterpFill    targetedTemplate = "interp_fill"        // interpolation FILL clause / 插值 FILL 子句
	targetedEveryDuration targetedTemplate = "every_duration"     // EVERY duration clause / EVERY 时长子句
	targetedDBPseudo      targetedTemplate = "db_pseudo"          // db-qualified table plus pseudo-column / 数据库限定表加伪列
	targetedSubquery      targetedTemplate = "subquery"           // FROM-subquery with WHERE/ORDER BY/LIMIT / 带 WHERE/ORDER BY/LIMIT 的 FROM 子查询
	targetedUnion         targetedTemplate = "union"              // UNION ALL of two selects / 两个 select 的 UNION ALL
	targetedFunction      targetedTemplate = "function"           // assorted scalar/cast functions / 各类标量/cast 函数
	targetedInsert        targetedTemplate = "insert_query"       // INSERT ... SELECT / INSERT ... SELECT
	targetedPartition     targetedTemplate = "partition"          // PARTITION BY clause / PARTITION BY 子句
)

// NextForRules tries to generate a statement that exercises the given missing grammar rules,
// ordering templates by relevance and returning the first one whose SQL parses cleanly.
// The bool result reports whether a usable statement was produced.
//
// NextForRules 尝试生成一条覆盖给定缺失语法规则的语句，
// 按相关性对模板排序，并返回第一条 SQL 能够干净解析的语句。
// 布尔返回值表示是否生成了可用的语句。
func (g *Generator) NextForRules(r *random.RNG, missingRules []string) (Generated, bool, error) {
	if r == nil {
		return Generated{}, false, fmt.Errorf("nil rng")
	}
	if len(missingRules) == 0 {
		return Generated{}, false, nil
	}
	order := targetedTemplateOrder(missingRules)
	for _, tpl := range order {
		sqlText := strings.TrimSpace(g.targetedSQL(tpl, r))
		if sqlText == "" {
			continue
		}
		if !strings.HasSuffix(sqlText, ";") {
			sqlText += ";"
		}
		out := Generated{SQL: sqlText, Tags: targetedTemplateTags(tpl)}
		if parsergate.Parse(out.SQL).Err == nil {
			return out, true, nil
		}
	}
	return Generated{}, false, nil
}

// targetedTemplateOrder scores templates by how well they match the missing rule names
// and returns all templates sorted by descending score (ties broken alphabetically).
//
// targetedTemplateOrder 按模板与缺失规则名称的匹配程度打分，
// 并返回按分数降序排列的所有模板（同分时按字母顺序排列）。
func targetedTemplateOrder(missingRules []string) []targetedTemplate {
	all := []targetedTemplate{
		targetedJoin,
		targetedParenJoin,
		targetedWindowFill,
		targetedStateWindow,
		targetedCountWindow,
		targetedInterpFill,
		targetedEveryDuration,
		targetedDBPseudo,
		targetedSubquery,
		targetedUnion,
		targetedFunction,
		targetedInsert,
		targetedPartition,
	}
	score := map[targetedTemplate]int{}
	for _, raw := range missingRules {
		rule := strings.ToLower(strings.TrimSpace(raw))
		switch {
		case strings.Contains(rule, "insert_query"):
			score[targetedInsert] += 8
		case strings.Contains(rule, "parenthesized_joined_table"):
			score[targetedParenJoin] += 10
			score[targetedJoin] += 4
		case strings.Contains(rule, "join"):
			score[targetedJoin] += 5
		case strings.Contains(rule, "count_window_args"), strings.Contains(rule, "trigger_col_name"), strings.Contains(rule, "column_name_list"):
			score[targetedCountWindow] += 8
			score[targetedInsert] += 5
		case strings.Contains(rule, "state_window_opt"), strings.Contains(rule, "extend_literal"), strings.Contains(rule, "zeroth_literal"):
			score[targetedStateWindow] += 8
		case strings.Contains(rule, "interp_fill_mode"), strings.Contains(rule, "fill_position_mode_extension"):
			score[targetedInterpFill] += 8
		case strings.Contains(rule, "duration_literal"), strings.Contains(rule, "every_opt"):
			score[targetedEveryDuration] += 7
		case strings.Contains(rule, "db_name"), strings.Contains(rule, "pseudo_column"):
			score[targetedDBPseudo] += 7
		case strings.Contains(rule, "window"), strings.Contains(rule, "fill"), strings.Contains(rule, "interval"), strings.Contains(rule, "session"), strings.Contains(rule, "range"), strings.Contains(rule, "every"):
			score[targetedWindowFill] += 5
		case strings.Contains(rule, "subquery"):
			score[targetedSubquery] += 4
		case strings.Contains(rule, "union"):
			score[targetedUnion] += 4
		case strings.Contains(rule, "partition"):
			score[targetedPartition] += 4
		case strings.Contains(rule, "function"), strings.Contains(rule, "func"), strings.Contains(rule, "expression"):
			score[targetedFunction] += 2
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		si, sj := score[all[i]], score[all[j]]
		if si != sj {
			return si > sj
		}
		return string(all[i]) < string(all[j])
	})
	return all
}

// targetedTemplateTags returns the grammar/feature tags associated with a template.
//
// targetedTemplateTags 返回与某个模板关联的语法/特性标签。
func targetedTemplateTags(t targetedTemplate) []string {
	switch t {
	case targetedJoin:
		return []string{"query_expression", "query_specification", "join", "from_clause", "where", "limit"}
	case targetedParenJoin:
		return []string{"query_expression", "query_specification", "joined_table", "parenthesized_joined_table", "from_clause"}
	case targetedWindowFill:
		return []string{"query_expression", "query_specification", "window", "fill", "function"}
	case targetedStateWindow:
		return []string{"query_expression", "query_specification", "window", "state_window_opt", "extend_literal", "zeroth_literal"}
	case targetedCountWindow:
		return []string{"query_expression", "query_specification", "window", "count_window_args", "column_name_list", "trigger_col_name"}
	case targetedInterpFill:
		return []string{"query_expression", "query_specification", "interp_fill_opt", "fill_position_mode_extension", "interp_fill_mode"}
	case targetedEveryDuration:
		return []string{"query_expression", "query_specification", "every_opt", "duration_literal"}
	case targetedDBPseudo:
		return []string{"query_expression", "query_specification", "db_name", "pseudo_column"}
	case targetedSubquery:
		return []string{"query_expression", "query_specification", "subquery", "where", "order_by", "limit"}
	case targetedUnion:
		return []string{"query_expression", "query_simple", "union_query_expression"}
	case targetedFunction:
		return []string{"query_expression", "query_specification", "function", "where"}
	case targetedInsert:
		return []string{"insert_query"}
	case targetedPartition:
		return []string{"query_expression", "query_specification", "partition", "limit"}
	default:
		return []string{"query_expression"}
	}
}

// targetedSQL builds the AST for a template and renders it to SQL text, returning "" when it yields nil.
//
// targetedSQL 为某个模板构建 AST 并渲染为 SQL 文本；当其返回 nil 时返回 ""。
func (g *Generator) targetedSQL(t targetedTemplate, r *random.RNG) string {
	stmt := g.targetedStatementAST(t, r)
	if stmt == nil {
		return ""
	}
	return sqlparser.SQLNodeToString(stmt)
}

// targetedStatementAST dispatches to the AST builder for the given template.
//
// targetedStatementAST 根据给定模板分派到相应的 AST 构建函数。
func (g *Generator) targetedStatementAST(t targetedTemplate, r *random.RNG) sqlparser.Statement {
	switch t {
	case targetedJoin:
		return g.targetedJoinAST(r)
	case targetedParenJoin:
		return g.targetedParenJoinAST(r)
	case targetedWindowFill:
		return g.targetedWindowFillAST(r)
	case targetedStateWindow:
		return g.targetedStateWindowAST(r)
	case targetedCountWindow:
		return g.targetedCountWindowAST(r)
	case targetedInterpFill:
		return g.targetedInterpFillAST(r)
	case targetedEveryDuration:
		return g.targetedEveryDurationAST(r)
	case targetedDBPseudo:
		return g.targetedDBPseudoAST(r)
	case targetedSubquery:
		return g.targetedSubqueryAST(r)
	case targetedUnion:
		return g.targetedUnionAST(r)
	case targetedFunction:
		return g.targetedFunctionAST(r)
	case targetedInsert:
		return g.targetedInsertAST(r)
	case targetedPartition:
		return g.targetedPartitionAST(r)
	default:
		return nil
	}
}

// targetedJoinAST builds an inner join of two tables on their time columns, with a WHERE and LIMIT.
//
// targetedJoinAST 按两张表的时间列构建一个内联接，并带有 WHERE 和 LIMIT。
func (g *Generator) targetedJoinAST(r *random.RNG) *sqlparser.SelectStmt {
	left, right := g.pickTwoTableNames(r)
	la := "a"
	ra := "b"
	numLeft := g.columnForTableKind(r, left, kindNumber)
	numRight := g.columnForTableKind(r, right, kindNumber)
	tsLeft := g.timeColumnForTable(left)
	tsRight := g.timeColumnForTable(right)

	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(numLeft, la),
			g.columnExprByName(numRight, ra),
		},
		From: &sqlparser.JoinTableExpr{
			Left:     &sqlparser.TableNameExpr{TableName: left, Alias: la},
			Right:    &sqlparser.TableNameExpr{TableName: right, Alias: ra},
			JoinType: sqlparser.JoinTypeInner,
			Condition: sqlparser.NewComparisonExpr(
				nil,
				g.columnExprByName(tsLeft, la),
				sqlparser.OP_TYPE_EQUAL,
				g.columnExprByName(tsRight, ra),
			),
		},
		Where: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(numLeft, la),
			sqlparser.OP_TYPE_GREATER_THAN,
			sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt},
		),
		Limit: &sqlparser.LimitExpr{Limit: sqlTok("20")},
	}
}

// targetedParenJoinAST builds a query whose FROM clause is a parenthesized join.
//
// targetedParenJoinAST 构建一个 FROM 子句为带括号联接的查询。
func (g *Generator) targetedParenJoinAST(r *random.RNG) *sqlparser.SelectStmt {
	left, right := g.pickTwoTableNames(r)
	la := "a"
	ra := "b"
	numLeft := g.columnForTableKind(r, left, kindNumber)
	tsLeft := g.timeColumnForTable(left)
	tsRight := g.timeColumnForTable(right)
	join := &sqlparser.JoinTableExpr{
		Left:     &sqlparser.TableNameExpr{TableName: left, Alias: la},
		Right:    &sqlparser.TableNameExpr{TableName: right, Alias: ra},
		JoinType: sqlparser.JoinTypeInner,
		Condition: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(tsLeft, la),
			sqlparser.OP_TYPE_EQUAL,
			g.columnExprByName(tsRight, ra),
		),
	}
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(numLeft, la),
		},
		From:  &sqlparser.ParenthesizedTableExpr{Inner: join},
		Limit: &sqlparser.LimitExpr{Limit: sqlTok("20")},
	}
}

// targetedWindowFillAST builds an INTERVAL window query with a PREV fill over an aggregate.
//
// targetedWindowFillAST 构建一个 INTERVAL 窗口查询，对聚合结果使用 PREV 填充。
func (g *Generator) targetedWindowFillAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			sqlparser.NewFuncExpr(nil, "avg", []sqlparser.Expr{g.columnExprByName(num, "")}),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		Window: sqlparser.WindowExpr{
			Interval: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
			Fill:     &sqlparser.FillExpr{Mode: sqlparser.FILL_MODE_PREV},
		},
	}
}

// targetedStateWindowAST builds a STATE_WINDOW query with extend and zeroth options and a TRUE_FOR duration.
//
// targetedStateWindowAST 构建一个 STATE_WINDOW 查询，带 extend 和 zeroth 选项以及 TRUE_FOR 时长。
func (g *Generator) targetedStateWindowAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, ""),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		Window: sqlparser.WindowExpr{
			StateWindow: g.columnExprByName(num, ""),
			StateWindowOpt: sqlparser.StateWindowOpt{
				HasExtend: true,
				Extend:    sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt},
				HasZeroth: true,
				Zeroth:    sqlparser.Literal{Val: sqlTok("z"), Type: sqlparser.LiteralString},
			},
			TrueFor: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
		},
	}
}

// targetedCountWindowAST builds a COUNT_WINDOW query with a slide and two distinct trigger columns.
//
// targetedCountWindowAST 构建一个 COUNT_WINDOW 查询，带滑动步长和两个不同的触发列。
func (g *Generator) targetedCountWindowAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	colA := g.columnForTableKind(r, table, kindAny)
	colB := g.columnForTableKind(r, table, kindAny)
	if strings.TrimSpace(colA) == "" {
		colA = "id"
	}
	if strings.TrimSpace(colB) == "" {
		colB = "v"
	}
	if strings.EqualFold(colA, colB) {
		colB = "tbname"
	}
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, ""),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		Window: sqlparser.WindowExpr{
			CountWindow:      sqlTok("10"),
			CountWindowSlide: sqlTok("2"),
			CountWindowCols:  []sqlparser.ColumnExpr{sqlparser.ColumnExpr(colA), sqlparser.ColumnExpr(colB)},
		},
	}
}

// targetedInterpFillAST builds a query with an interpolation FILL clause (NEAR mode).
//
// targetedInterpFillAST 构建一个带插值 FILL 子句（NEAR 模式）的查询。
func (g *Generator) targetedInterpFillAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, ""),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		InterpFill: &sqlparser.FillExpr{
			Mode:   sqlparser.FILL_MODE_NEAR,
			Values: []sqlparser.Expr{sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt}},
		},
	}
}

// targetedEveryDurationAST builds a query with an EVERY duration clause.
//
// targetedEveryDurationAST 构建一个带 EVERY 时长子句的查询。
func (g *Generator) targetedEveryDurationAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, ""),
		},
		From:  &sqlparser.TableNameExpr{TableName: table},
		Every: sqlparser.Literal{Val: sqlTok(pick(r, g.durationLit)), Type: sqlparser.LiteralDuration},
	}
}

// targetedDBPseudoAST builds a query against a db-qualified table that also selects the tbname pseudo-column.
//
// targetedDBPseudoAST 构建一个针对数据库限定表的查询，同时选取 tbname 伪列。
func (g *Generator) targetedDBPseudoAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			sqlparser.NewPseudoColumnExpr(nil, "tbname"),
			g.columnExprByName(num, ""),
		},
		From: &sqlparser.TableNameExpr{DBName: "db1", TableName: table},
	}
}

// targetedSubqueryAST builds a query that selects from a derived table (FROM-subquery)
// with WHERE on both levels plus ORDER BY and LIMIT.
//
// targetedSubqueryAST 构建一个从派生表（FROM 子查询）中查询的语句，
// 在两个层级上都带 WHERE，外加 ORDER BY 和 LIMIT。
func (g *Generator) targetedSubqueryAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	ts := g.timeColumnForTable(table)

	inner := &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, ""),
			g.columnExprByName(ts, ""),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		Where: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(num, ""),
			sqlparser.OP_TYPE_GREATER_THAN,
			sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt},
		),
	}
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			g.columnExprByName(num, "s"),
		},
		From: &sqlparser.SubqueryTableExpr{Query: inner, Alias: "s"},
		Where: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(num, "s"),
			sqlparser.OP_TYPE_GREATER_THAN,
			sqlparser.Literal{Val: sqlTok("2"), Type: sqlparser.LiteralInt},
		),
		OrderBy: []sqlparser.OrderByExpr{{
			Expr:       g.columnExprByName(num, "s"),
			Asc:        true,
			NullsFirst: false,
		}},
		Limit: &sqlparser.LimitExpr{Limit: sqlTok("10")},
	}
}

// targetedUnionAST builds a UNION ALL of two filtered single-column selects over different tables.
//
// targetedUnionAST 构建两个对不同表、各带过滤条件的单列 select 的 UNION ALL。
func (g *Generator) targetedUnionAST(r *random.RNG) *sqlparser.SelectStmt {
	left, right := g.pickTwoTableNames(r)
	numLeft := g.columnForTableKind(r, left, kindNumber)
	numRight := g.columnForTableKind(r, right, kindNumber)
	return &sqlparser.SelectStmt{
		Left: &sqlparser.SelectStmt{
			Select: []sqlparser.Expr{g.columnExprByName(numLeft, "")},
			From:   &sqlparser.TableNameExpr{TableName: left},
			Where: sqlparser.NewComparisonExpr(
				nil,
				g.columnExprByName(numLeft, ""),
				sqlparser.OP_TYPE_GREATER_THAN,
				sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt},
			),
		},
		Right: &sqlparser.SelectStmt{
			Select: []sqlparser.Expr{g.columnExprByName(numRight, "")},
			From:   &sqlparser.TableNameExpr{TableName: right},
			Where: sqlparser.NewComparisonExpr(
				nil,
				g.columnExprByName(numRight, ""),
				sqlparser.OP_TYPE_GREATER_THAN,
				sqlparser.Literal{Val: sqlTok("2"), Type: sqlparser.LiteralInt},
			),
		},
		SetOp:  "union",
		SetAll: true,
	}
}

// targetedFunctionAST builds a query selecting several scalar functions (abs/round/lower/cast) with a WHERE.
//
// targetedFunctionAST 构建一个选取若干标量函数（abs/round/lower/cast）并带 WHERE 的查询。
func (g *Generator) targetedFunctionAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	str := g.columnForTableKind(r, table, kindString)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{
			sqlparser.NewFuncExpr(nil, "abs", []sqlparser.Expr{g.columnExprByName(num, "")}),
			sqlparser.NewFuncExpr(nil, "round", []sqlparser.Expr{
				g.columnExprByName(num, ""),
				sqlparser.Literal{Val: sqlTok("2"), Type: sqlparser.LiteralInt},
			}),
			sqlparser.NewFuncExpr(nil, "lower", []sqlparser.Expr{g.columnExprByName(str, "")}),
			sqlparser.NewCastExpr(nil, g.columnExprByName(num, ""), "bigint"),
		},
		From: &sqlparser.TableNameExpr{TableName: table},
		Where: sqlparser.NewComparisonExpr(
			nil,
			g.columnExprByName(num, ""),
			sqlparser.OP_TYPE_GREATER_THAN,
			sqlparser.Literal{Val: sqlTok("1"), Type: sqlparser.LiteralInt},
		),
	}
}

// targetedInsertAST builds an "INSERT INTO ... SELECT" statement, optionally db-qualified,
// copying two columns (possibly including tbname) from a source table.
//
// targetedInsertAST 构建一条 "INSERT INTO ... SELECT" 语句（可选带数据库限定），
// 从源表复制两列（可能包含 tbname）。
func (g *Generator) targetedInsertAST(r *random.RNG) *sqlparser.InsertQueryStmt {
	dst, src := g.pickTwoTableNames(r)
	insertTable := dst
	if chance(r, 35) {
		insertTable = "db1." + dst
	}
	colA := g.columnForTableKind(r, dst, kindAny)
	colB := g.columnForTableKind(r, dst, kindAny)
	if strings.TrimSpace(colA) == "" {
		colA = "id"
	}
	if strings.TrimSpace(colB) == "" || strings.EqualFold(colA, colB) {
		colB = "tbname"
	}
	srcA := g.columnForTableKind(r, src, kindAny)
	srcB := g.columnForTableKind(r, src, kindAny)
	if strings.TrimSpace(srcA) == "" {
		srcA = "id"
	}
	if strings.TrimSpace(srcB) == "" {
		srcB = "v"
	}
	return &sqlparser.InsertQueryStmt{
		Table:   insertTable,
		Columns: []string{colA, colB},
		Query: &sqlparser.SelectStmt{
			Select: []sqlparser.Expr{
				g.columnExprByName(srcA, ""),
				func() sqlparser.Expr {
					if strings.EqualFold(colB, "tbname") {
						return sqlparser.NewPseudoColumnExpr(nil, "tbname")
					}
					return g.columnExprByName(srcB, "")
				}(),
			},
			From:  &sqlparser.TableNameExpr{TableName: src},
			Limit: &sqlparser.LimitExpr{Limit: sqlTok("1")},
		},
	}
}

// targetedPartitionAST builds a query with a PARTITION BY clause and a LIMIT.
//
// targetedPartitionAST 构建一个带 PARTITION BY 子句和 LIMIT 的查询。
func (g *Generator) targetedPartitionAST(r *random.RNG) *sqlparser.SelectStmt {
	table := g.tableRefName(r)
	num := g.columnForTableKind(r, table, kindNumber)
	part := g.columnForTableKind(r, table, kindAny)
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{g.columnExprByName(num, "")},
		From:   &sqlparser.TableNameExpr{TableName: table},
		Partition: &sqlparser.RawExpr{
			Kind: "partition_by",
			Args: []sqlparser.Expr{g.columnExprByName(part, "")},
		},
		Limit: &sqlparser.LimitExpr{Limit: sqlTok("10")},
	}
}

// columnForTableKind returns a column of the requested kind for the given table, preferring
// the table's own columns, then falling back to the generator pools or fixed defaults.
//
// columnForTableKind 返回给定表中所请求类别的列，优先使用该表自身的列，
// 然后回退到生成器的列池或固定的默认值。
func (g *Generator) columnForTableKind(r *random.RNG, table string, kind valueKind) string {
	if r != nil && g.tableCols != nil {
		if cols, ok := g.tableCols[table]; ok && len(cols) > 0 {
			candidates := make([]string, 0, len(cols))
			for _, c := range cols {
				if kind == kindAny || c.Kind == kind || (kind == kindNumber && c.Kind == kindAny) {
					candidates = append(candidates, c.Name)
				}
			}
			if len(candidates) > 0 {
				return candidates[r.Intn(len(candidates))]
			}
			return cols[r.Intn(len(cols))].Name
		}
	}
	if r != nil {
		return g.columnRefKind(r, kind)
	}
	switch kind {
	case kindNumber:
		return "v"
	case kindString:
		return "b"
	case kindTime:
		return "ts"
	default:
		return "id"
	}
}
