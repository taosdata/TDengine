package querygen

import (
	"fmt"
	"sort"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

type targetedTemplate string

const (
	targetedJoin          targetedTemplate = "join"
	targetedParenJoin     targetedTemplate = "parenthesized_join"
	targetedWindowFill    targetedTemplate = "window_fill"
	targetedStateWindow   targetedTemplate = "state_window"
	targetedCountWindow   targetedTemplate = "count_window"
	targetedInterpFill    targetedTemplate = "interp_fill"
	targetedEveryDuration targetedTemplate = "every_duration"
	targetedDBPseudo      targetedTemplate = "db_pseudo"
	targetedSubquery      targetedTemplate = "subquery"
	targetedUnion         targetedTemplate = "union"
	targetedFunction      targetedTemplate = "function"
	targetedInsert        targetedTemplate = "insert_query"
	targetedPartition     targetedTemplate = "partition"
)

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

func (g *Generator) targetedSQL(t targetedTemplate, r *random.RNG) string {
	stmt := g.targetedStatementAST(t, r)
	if stmt == nil {
		return ""
	}
	return sqlparser.SQLNodeToString(stmt)
}

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
