package querygen

import (
	"fmt"
	"sort"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

type Config struct {
	MaxDepth       int
	MaxSelectItems int
	MaxExprDepth   int
}

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name    string
	Columns []Column
}

type Schema struct {
	Tables []Table
}

type valueKind int

const (
	kindAny valueKind = iota
	kindNumber
	kindString
	kindBool
	kindTime
	kindJSON
)

type columnRef struct {
	Table string
	Name  string
	Kind  valueKind
}

type Generated struct {
	SQL  string
	Tags []string
}

type Generator struct {
	cfg         Config
	tables      []string
	columns     []string
	typedCols   map[valueKind][]columnRef
	tableCols   map[string][]columnRef
	aliases     []string
	funcNames   []string
	pseudoCols  []string
	durationLit []string
}

type genCtx struct {
	tags map[string]struct{}
}

func (c *genCtx) add(tag string) {
	if strings.TrimSpace(tag) == "" {
		return
	}
	c.tags[tag] = struct{}{}
}

func (c *genCtx) list() []string {
	out := make([]string, 0, len(c.tags))
	for k := range c.tags {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func DefaultConfig() Config {
	return Config{MaxDepth: 3, MaxSelectItems: 4, MaxExprDepth: 3}
}

func New(cfg Config) *Generator {
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 3
	}
	if cfg.MaxSelectItems <= 0 {
		cfg.MaxSelectItems = 4
	}
	if cfg.MaxExprDepth <= 0 {
		cfg.MaxExprDepth = 3
	}
	g := &Generator{
		cfg:     cfg,
		tables:  []string{"t1", "t2", "t3"},
		columns: []string{"ts", "id", "v", "c1", "c2", "u1", "bi", "ubi", "f", "d", "si", "usi", "ti", "uti", "ok", "a", "b", "n", "vb", "geo", "de"},
		aliases: []string{"a", "b", "c", "x", "y", "z"},
		funcNames: []string{
			"abs", "ceil", "floor", "round", "pow", "length", "lower", "upper", "concat",
			"sum", "avg", "min", "max",
		},
		pseudoCols:  []string{"tbname", "rowts", "qstart", "qend", "qduration", "wstart", "wend", "wduration", "irowts", "isfilled"},
		durationLit: []string{"1s", "5s", "10s", "30s", "1m", "5m"},
	}
	g.BindSchema(defaultSchema())
	return g
}

func (g *Generator) BindSchema(schema Schema) {
	if len(schema.Tables) == 0 {
		return
	}
	tables := make([]string, 0, len(schema.Tables))
	colSet := make(map[string]struct{}, 32)
	tableCols := make(map[string][]columnRef, len(schema.Tables))
	typed := map[valueKind][]columnRef{
		kindAny:    {},
		kindNumber: {},
		kindString: {},
		kindBool:   {},
		kindTime:   {},
		kindJSON:   {},
	}

	for _, t := range schema.Tables {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		tables = append(tables, name)
		for _, c := range t.Columns {
			col := strings.TrimSpace(c.Name)
			if col == "" {
				continue
			}
			k := inferKind(c.Type)
			ref := columnRef{Table: name, Name: col, Kind: k}
			tableCols[name] = append(tableCols[name], ref)
			typed[k] = append(typed[k], ref)
			if k != kindTime && k != kindAny {
				typed[kindAny] = append(typed[kindAny], ref)
			}
			if _, ok := colSet[col]; !ok {
				colSet[col] = struct{}{}
			}
		}
	}
	if len(tables) == 0 || len(typed[kindAny]) == 0 {
		return
	}
	sort.Strings(tables)
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	g.tables = tables
	g.columns = cols
	g.tableCols = tableCols
	g.typedCols = typed
}

func defaultSchema() Schema {
	cols := []Column{
		{Name: "ts", Type: "timestamp"},
		{Name: "id", Type: "int"},
		{Name: "v", Type: "int"},
		{Name: "c1", Type: "int"},
		{Name: "c2", Type: "int"},
		{Name: "u1", Type: "int unsigned"},
		{Name: "bi", Type: "bigint"},
		{Name: "ubi", Type: "bigint unsigned"},
		{Name: "f", Type: "float"},
		{Name: "d", Type: "double"},
		{Name: "si", Type: "smallint"},
		{Name: "usi", Type: "smallint unsigned"},
		{Name: "ti", Type: "tinyint"},
		{Name: "uti", Type: "tinyint unsigned"},
		{Name: "ok", Type: "bool"},
		{Name: "a", Type: "binary(32)"},
		{Name: "b", Type: "varchar(64)"},
		{Name: "n", Type: "nchar(32)"},
		{Name: "vb", Type: "varbinary(64)"},
		{Name: "geo", Type: "geometry(100)"},
		{Name: "de", Type: "decimal(18,6)"},
	}
	copyCols := func() []Column {
		out := make([]Column, len(cols))
		copy(out, cols)
		return out
	}
	return Schema{
		Tables: []Table{
			{Name: "t1", Columns: copyCols()},
			{Name: "t2", Columns: copyCols()},
			{Name: "t3", Columns: copyCols()},
		},
	}
}

func inferKind(typ string) valueKind {
	t := strings.ToLower(strings.TrimSpace(typ))
	switch {
	case strings.Contains(t, "timestamp"), strings.Contains(t, "datetime"), strings.Contains(t, "date"):
		return kindTime
	case strings.Contains(t, "bool"):
		return kindBool
	case strings.Contains(t, "json"):
		return kindJSON
	case strings.Contains(t, "geometry"):
		return kindAny
	case strings.Contains(t, "varbinary"), strings.Contains(t, "binary"), strings.Contains(t, "varchar"), strings.Contains(t, "nchar"), strings.Contains(t, "text"), strings.Contains(t, "char"):
		return kindString
	case strings.Contains(t, "utinyint"), strings.Contains(t, "usmallint"), strings.Contains(t, "uint"), strings.Contains(t, "ubigint"), strings.Contains(t, "tinyint"), strings.Contains(t, "smallint"), strings.Contains(t, "int"), strings.Contains(t, "bigint"), strings.Contains(t, "float"), strings.Contains(t, "double"), strings.Contains(t, "decimal"), strings.Contains(t, "numeric"):
		return kindNumber
	default:
		return kindAny
	}
}

func (g *Generator) Next(r *random.RNG) (Generated, error) {
	if r == nil {
		return Generated{}, fmt.Errorf("nil rng")
	}
	var last Generated
	for i := 0; i < 16; i++ {
		ctx := &genCtx{tags: map[string]struct{}{}}
		depth := g.cfg.MaxDepth
		var stmt sqlparser.Statement
		if chance(r, 18) {
			stmt = g.insertQueryAST(r, depth, ctx)
		} else {
			stmt = g.queryExpressionAST(r, depth, ctx)
		}
		sqlText := strings.TrimSpace(sqlparser.SQLNodeToString(stmt))
		sqlText = strings.TrimSpace(sqlText)
		if sqlText == "" {
			continue
		}
		if !strings.HasSuffix(sqlText, ";") {
			sqlText += ";"
		}
		out := Generated{SQL: sqlText, Tags: ctx.list()}
		last = out
		if parsergate.Parse(sqlText).Err == nil {
			return out, nil
		}
	}
	if strings.TrimSpace(last.SQL) == "" {
		return Generated{}, fmt.Errorf("empty generated SQL")
	}
	return last, nil
}

func (g *Generator) queryExpression(r *random.RNG, depth int, ctx *genCtx) string {
	stmt := g.queryExpressionAST(r, depth, ctx)
	if stmt == nil {
		return ""
	}
	return strings.TrimSpace(sqlparser.SQLNodeToString(stmt))
}

func (g *Generator) queryExpressionAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	ctx.add("query_expression")
	q := g.querySimpleAST(r, depth, ctx)
	if q == nil {
		q = fallbackSelectStmt()
	}
	if chance(r, 55) {
		q.OrderBy = g.orderByAST(r, depth, ctx)
	}
	if chance(r, 28) {
		q.SLimit = g.slimitAST(r, ctx)
	}
	if chance(r, 45) {
		q.Limit = g.limitAST(r, ctx)
	}
	return q
}

func (g *Generator) querySimpleAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	ctx.add("query_simple")
	if depth > 0 && chance(r, 30) {
		ctx.add("union_query_expression")
		left := g.querySimpleOrSubqueryAST(r, depth-1, ctx)
		right := g.querySimpleOrSubqueryAST(r, depth-1, ctx)
		if left == nil {
			left = fallbackSelectStmt()
		}
		if right == nil {
			right = fallbackSelectStmt()
		}
		return &sqlparser.SelectStmt{
			Left:   left,
			Right:  right,
			SetOp:  "union",
			SetAll: chance(r, 55),
		}
	}
	return g.querySpecificationAST(r, depth, ctx)
}

func (g *Generator) querySimpleOrSubqueryAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	ctx.add("query_simple_or_subquery")
	if depth > 0 && chance(r, 28) {
		ctx.add("subquery")
		inner := g.queryExpressionAST(r, depth-1, ctx)
		if inner == nil {
			return fallbackSelectStmt()
		}
		return inner
	}
	if depth > 0 && chance(r, 25) {
		return g.querySimpleAST(r, depth-1, ctx)
	}
	return g.querySpecificationAST(r, depth, ctx)
}

func (g *Generator) querySpecificationAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	stmt := g.querySpecificationASTPure(r, depth, ctx)
	if stmt == nil {
		return fallbackSelectStmt()
	}
	return stmt
}

func (g *Generator) querySpecificationSQL(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("query_specification")
	if chance(r, 35) {
		ctx.add("safe_simple")
		return g.safeSimpleQuerySpecSQL(r, ctx)
	}
	enableFrom := true
	enableWhere := chance(r, 56)
	enablePartition := chance(r, 22)
	enableRange := false
	enableEvery := false
	enableInterpFill := false
	enableWindow := chance(r, 30)
	enableGroup := chance(r, 35)
	windowClause := ""
	windowKind := ""
	if enableWindow {
		windowClause, windowKind = g.twindowClause(r, depth, ctx)
	}
	parts := []string{"select"}
	if chance(r, 25) {
		ctx.add("hint")
		parts = append(parts, g.hintClause(r, enableFrom, enableWindow))
	}
	if chance(r, 22) {
		if chance(r, 55) {
			parts = append(parts, "distinct")
		} else {
			parts = append(parts, "all")
		}
	}
	if chance(r, 12) {
		ctx.add("tag_mode")
		parts = append(parts, "tags")
	}
	var groupedExprs []string
	if enableGroup {
		groupedExprs = g.groupByExprList(r, ctx)
		parts = append(parts, g.groupedSelectList(r, depth, ctx, groupedExprs))
	} else {
		parts = append(parts, g.selectList(r, depth, ctx))
	}

	if enableFrom {
		parts = append(parts, g.fromClause(r, depth, ctx))
	}
	if enableWhere {
		parts = append(parts, g.whereClause(r, depth, ctx))
	}
	if enablePartition {
		parts = append(parts, g.partitionClause(r, depth, ctx))
	}
	if enableRange && windowKind == "interval" {
		parts = append(parts, g.rangeClause(r, depth, ctx))
	}
	if enableEvery && windowKind == "interval" {
		parts = append(parts, g.everyClause(r, ctx))
	}
	if enableInterpFill && windowKind == "interval" {
		parts = append(parts, g.interpFillClause(r, depth, ctx))
	}
	if windowClause != "" {
		enableGroup = false
		parts = append(parts, windowClause)
	}
	if enableGroup {
		parts = append(parts, "group by "+strings.Join(groupedExprs, ", "))
		if chance(r, 50) {
			parts = append(parts, g.groupedHavingClause(r, ctx))
		}
	} else if chance(r, 10) {
		parts = append(parts, g.havingClause(r, depth, ctx))
	}
	return strings.Join(parts, " ")
}

func (g *Generator) safeSimpleQuerySpecSQL(r *random.RNG, ctx *genCtx) string {
	table := g.tableRefName(r)
	if strings.TrimSpace(table) == "" {
		table = "t1"
	}
	items := make([]string, 0, 3)
	n := 1 + r.Intn(3)
	for i := 0; i < n; i++ {
		items = append(items, g.columnName(r))
	}
	sqlText := "select " + strings.Join(items, ", ") + " from " + table
	if chance(r, 55) {
		sqlText += " where " + g.columnRefKind(r, kindNumber) + " > " + g.unsignedInt(r)
	}
	if chance(r, 45) {
		sqlText += " order by " + g.columnName(r)
		if chance(r, 50) {
			sqlText += " desc"
		} else {
			sqlText += " asc"
		}
	}
	if chance(r, 60) {
		sqlText += " " + g.limitClause(r, ctx)
	}
	return sqlText
}

func reparseSelectWithClause(base *sqlparser.SelectStmt, clause string) *sqlparser.SelectStmt {
	if base == nil {
		return nil
	}
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return base
	}
	baseSQL := strings.TrimSpace(sqlparser.SQLNodeToString(base))
	if baseSQL == "" {
		return base
	}
	if parsed := parseSelectStmt(baseSQL + " " + clause); parsed != nil {
		return parsed
	}
	return base
}

func parseSelectStmt(sqlText string) *sqlparser.SelectStmt {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return nil
	}
	stmt, err := sqlparser.Parse(sqlText)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*sqlparser.SelectStmt)
	if !ok {
		return nil
	}
	return sel
}

func fallbackSelectStmt() *sqlparser.SelectStmt {
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{&sqlparser.StarExpr{}},
		From:   &sqlparser.TableNameExpr{TableName: "t1"},
	}
}

func (g *Generator) hintClause(r *random.RNG, hasFrom bool, hasWindow bool) string {
	hints := make([]string, 0, 6)
	hints = append(hints, "batch_scan()", "no_batch_scan()", "sort_for_group()", "partition_first()", "skip_tsma()")
	if hasFrom {
		hints = append(hints, "hash_join()")
	}
	if hasWindow {
		if chance(r, 50) {
			hints = append(hints, "win_optimize_batch()")
		} else {
			hints = append(hints, "win_optimize_single()")
		}
	}
	return "/*+ " + pick(r, hints) + " */"
}

func (g *Generator) selectList(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("select_list")
	n := 1 + r.Intn(g.cfg.MaxSelectItems)
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, g.selectItem(r, depth, ctx))
	}
	return strings.Join(items, ", ")
}

func (g *Generator) selectItem(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("select_item")
	switch r.Intn(8) {
	case 0:
		return "*"
	case 1:
		return "*"
	default:
		expr := g.commonExpression(r, depth, ctx)
		if chance(r, 38) {
			alias := g.alias(r)
			if chance(r, 50) {
				return expr + " as " + alias
			}
			return expr + " " + alias
		}
		return expr
	}
}

func (g *Generator) fromClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("from_clause")
	count := 1
	refs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		refs = append(refs, g.tableReference(r, depth, ctx))
	}
	return "from " + strings.Join(refs, ", ")
}

func (g *Generator) tableReference(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("table_reference")
	if depth > 0 && chance(r, 10) {
		ctx.add("join")
		return g.joinedTable(r, depth-1, ctx)
	}
	return g.tablePrimary(r, depth, ctx)
}

func (g *Generator) tablePrimary(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("table_primary")
	switch {
	case depth > 0 && chance(r, 24):
		ctx.add("subquery")
		return "(" + g.queryExpression(r, depth-1, ctx) + ") " + g.alias(r)
	case depth > 0 && chance(r, 10):
		ctx.add("parenthesized_joined_table")
		return "(" + g.joinedTable(r, depth-1, ctx) + ")"
	default:
		t := g.tableRefName(r)
		if chance(r, 60) {
			if chance(r, 45) {
				return t + " as " + g.alias(r)
			}
			return t + " " + g.alias(r)
		}
		return t
	}
}

func (g *Generator) joinedTable(r *random.RNG, depth int, ctx *genCtx) string {
	lt, rt := g.pickTwoTableNames(r)
	left := lt
	right := rt
	on := " on " + g.timeJoinCondition(lt, rt)
	switch r.Intn(12) {
	case 0:
		return left + " join " + right + on
	case 1:
		return left + " inner join " + right + on
	case 2:
		return left + " left join " + right + on
	case 3:
		return left + " right join " + right + on
	case 4:
		return left + " full join " + right + on
	case 5:
		return left + " left outer join " + right + on
	case 6:
		return left + " right outer join " + right + on
	case 7:
		return left + " left semi join " + right + on
	case 8:
		return left + " right anti join " + right + on
	case 9:
		out := left + " left asof join " + right + on
		if chance(r, 35) {
			out += " jlimit " + g.unsignedInt(r)
		}
		return out
	case 10:
		out := left + " right asof join " + right + on
		if chance(r, 35) {
			out += " jlimit " + g.unsignedInt(r)
		}
		return out
	default:
		out := left
		if chance(r, 50) {
			out += " left window join "
		} else {
			out += " right window join "
		}
		out += right
		out += on
		out += " window_offset(" + pick(r, g.durationLit) + ", -" + pick(r, g.durationLit) + ")"
		if chance(r, 35) {
			out += " jlimit " + g.unsignedInt(r)
		}
		return out
	}
}

func (g *Generator) whereClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("where")
	return "where " + g.searchCondition(r, depth, ctx)
}

func (g *Generator) groupByClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("group_by")
	exprs := g.groupByExprList(r, ctx)
	return "group by " + strings.Join(exprs, ", ")
}

func (g *Generator) groupByExprList(r *random.RNG, ctx *genCtx) []string {
	ctx.add("group_by")
	n := 1 + r.Intn(2)
	exprs := make([]string, 0, n)
	seen := map[string]struct{}{}
	for len(exprs) < n {
		e := g.columnRefKind(r, kindAny)
		if strings.TrimSpace(e) == "" {
			e = "id"
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		exprs = append(exprs, e)
	}
	return exprs
}

func (g *Generator) groupedSelectList(r *random.RNG, depth int, ctx *genCtx, groupedExprs []string) string {
	ctx.add("select_list")
	ctx.add("select_item")

	items := make([]string, 0, len(groupedExprs)+2)
	for _, e := range groupedExprs {
		if chance(r, 35) {
			items = append(items, e+" as "+g.alias(r))
		} else {
			items = append(items, e)
		}
	}

	aggCount := 1 + r.Intn(2)
	for i := 0; i < aggCount; i++ {
		switch r.Intn(4) {
		case 0:
			items = append(items, "count(*)")
		case 1:
			items = append(items, "sum("+g.columnRefKind(r, kindNumber)+")")
		case 2:
			items = append(items, "max("+g.columnRefKind(r, kindNumber)+")")
		default:
			items = append(items, "avg("+g.columnRefKind(r, kindNumber)+")")
		}
	}
	return strings.Join(items, ", ")
}

func (g *Generator) groupedHavingClause(r *random.RNG, ctx *genCtx) string {
	ctx.add("having")
	switch r.Intn(3) {
	case 0:
		return "having count(*) > " + g.unsignedInt(r)
	case 1:
		return "having sum(" + g.columnRefKind(r, kindNumber) + ") > " + g.unsignedInt(r)
	default:
		return "having max(" + g.columnRefKind(r, kindNumber) + ") is not null"
	}
}

func (g *Generator) havingClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("having")
	return "having " + g.searchCondition(r, depth, ctx)
}

func (g *Generator) partitionClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("partition")
	n := 1 + r.Intn(2)
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		x := g.exprOrSubquery(r, depth, ctx)
		if chance(r, 25) {
			if chance(r, 50) {
				x += " " + g.alias(r)
			} else {
				x += " as " + g.alias(r)
			}
		}
		items = append(items, x)
	}
	return "partition by " + strings.Join(items, ", ")
}

func (g *Generator) rangeClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("range")
	n := 1 + r.Intn(2)
	exprs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		exprs = append(exprs, g.exprOrSubqueryKind(r, depth, ctx, kindTime))
	}
	return "range(" + strings.Join(exprs, ", ") + ")"
}

func (g *Generator) everyClause(r *random.RNG, ctx *genCtx) string {
	ctx.add("every")
	return "every(" + pick(r, g.durationLit) + ")"
}

func (g *Generator) interpFillClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("fill")
	exprs := g.expressionList(r, min(depth, 1), ctx)
	switch r.Intn(5) {
	case 0:
		return "fill(value, " + exprs + ")"
	case 1:
		return "fill(value_f, " + exprs + ")"
	case 2:
		return "fill(prev, " + exprs + ")"
	case 3:
		return "fill(near, " + exprs + ")"
	default:
		return "fill(" + pick(r, []string{"none", "null", "null_f", "linear", "prev", "next", "near"}) + ")"
	}
}

func (g *Generator) twindowClause(r *random.RNG, depth int, ctx *genCtx) (string, string) {
	ctx.add("window")
	switch r.Intn(4) {
	case 0:
		out := "interval(" + pick(r, g.durationLit) + ")"
		if chance(r, 45) {
			out += " " + g.fillOpt(r, depth, ctx)
		}
		return out, "interval"
	case 1:
		out := "interval(" + pick(r, g.durationLit) + ", " + pick(r, g.durationLit) + ")"
		if chance(r, 45) {
			out += " " + g.fillOpt(r, depth, ctx)
		}
		return out, "interval"
	case 2:
		return "session(" + g.columnRefKind(r, kindTime) + ", " + pick(r, g.durationLit) + ")", "session"
	default:
		return "state_window(" + g.columnRefKind(r, kindNumber) + ")", "state"
	}
}

func (g *Generator) fillOpt(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("fill")
	switch r.Intn(3) {
	case 0:
		return "fill(value, " + g.expressionList(r, min(depth, 1), ctx) + ")"
	case 1:
		return "fill(value_f, " + g.expressionList(r, min(depth, 1), ctx) + ")"
	default:
		return "fill(" + pick(r, []string{"none", "null", "null_f", "linear", "prev", "next"}) + ")"
	}
}

func (g *Generator) orderByClause(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("order_by")
	n := 1 + r.Intn(2)
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		expr := g.exprOrSubquery(r, depth, ctx)
		if chance(r, 60) {
			if chance(r, 50) {
				expr += " asc"
			} else {
				expr += " desc"
			}
		}
		if chance(r, 30) {
			if chance(r, 50) {
				expr += " nulls first"
			} else {
				expr += " nulls last"
			}
		}
		items = append(items, expr)
	}
	return "order by " + strings.Join(items, ", ")
}

func (g *Generator) limitClause(r *random.RNG, ctx *genCtx) string {
	ctx.add("limit")
	l := g.unsignedInt(r)
	o := g.unsignedInt(r)
	switch r.Intn(3) {
	case 0:
		return "limit " + l
	case 1:
		return "limit " + l + " offset " + o
	default:
		return "limit " + o + ", " + l
	}
}

func (g *Generator) slimitClause(r *random.RNG, ctx *genCtx) string {
	ctx.add("slimit")
	l := g.unsignedInt(r)
	o := g.unsignedInt(r)
	switch r.Intn(3) {
	case 0:
		return "slimit " + l
	case 1:
		return "slimit " + l + " soffset " + o
	default:
		return "slimit " + o + ", " + l
	}
}

func (g *Generator) searchCondition(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("search_condition")
	if depth <= 0 {
		if chance(r, 40) {
			return g.commonExpression(r, 0, ctx)
		}
		return g.booleanPrimary(r, 0, ctx)
	}
	switch r.Intn(5) {
	case 0:
		return "not " + g.booleanPrimary(r, depth-1, ctx)
	case 1:
		return g.searchCondition(r, depth-1, ctx) + " and " + g.searchCondition(r, depth-1, ctx)
	case 2:
		return g.searchCondition(r, depth-1, ctx) + " or " + g.searchCondition(r, depth-1, ctx)
	default:
		if chance(r, 35) {
			return g.commonExpression(r, depth-1, ctx)
		}
		return g.booleanPrimary(r, depth-1, ctx)
	}
}

func (g *Generator) booleanPrimary(r *random.RNG, depth int, ctx *genCtx) string {
	if depth > 0 && chance(r, 20) {
		return "(" + g.searchCondition(r, depth-1, ctx) + ")"
	}
	return g.predicate(r, depth, ctx)
}

func (g *Generator) predicate(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("predicate")
	kind := g.pickPredicateKind(r)
	left := g.exprOrSubqueryKind(r, depth, ctx, kind)
	right := g.exprOrSubqueryKind(r, depth, ctx, kind)
	switch r.Intn(8) {
	case 0:
		return left + " " + pick(r, []string{"<", ">", "<=", ">=", "!=", "=", "like", "not like", "match", "nmatch", "regexp", "not regexp", "contains"}) + " " + right
	case 1:
		return left + " between " + right + " and " + g.exprOrSubqueryKind(r, depth, ctx, kind)
	case 2:
		return left + " not between " + right + " and " + g.exprOrSubqueryKind(r, depth, ctx, kind)
	case 3:
		return left + " is null"
	case 4:
		return left + " is not null"
	case 5:
		return "isnull(" + left + ")"
	case 6:
		return "isnotnull(" + left + ")"
	default:
		vals := []string{g.literalOfKind(r, kind), g.literalOfKind(r, kind), g.literalOfKind(r, kind)}
		op := "in"
		if chance(r, 45) {
			op = "not in"
		}
		return left + " " + op + " (" + strings.Join(vals, ", ") + ")"
	}
}

func (g *Generator) commonExpression(r *random.RNG, depth int, ctx *genCtx) string {
	if depth > 0 && chance(r, 25) {
		return g.searchCondition(r, depth-1, ctx)
	}
	return g.exprOrSubqueryKind(r, depth, ctx, kindAny)
}

func (g *Generator) exprOrSubquery(r *random.RNG, depth int, ctx *genCtx) string {
	return g.exprOrSubqueryKind(r, depth, ctx, kindAny)
}

func (g *Generator) exprOrSubqueryKind(r *random.RNG, depth int, ctx *genCtx, expect valueKind) string {
	return g.expressionKind(r, depth, ctx, expect)
}

func (g *Generator) expression(r *random.RNG, depth int, ctx *genCtx) string {
	return g.expressionKind(r, depth, ctx, kindAny)
}

func (g *Generator) expressionKind(r *random.RNG, depth int, ctx *genCtx, expect valueKind) string {
	if depth <= 0 {
		return g.terminalExpressionKind(r, ctx, expect)
	}
	switch r.Intn(12) {
	case 0:
		return g.terminalExpressionKind(r, ctx, expect)
	case 1:
		return "(" + g.expressionKind(r, depth-1, ctx, expect) + ")"
	case 2:
		return "+(" + g.exprOrSubqueryKind(r, depth-1, ctx, kindNumber) + ")"
	case 3:
		return "-(" + g.exprOrSubqueryKind(r, depth-1, ctx, kindNumber) + ")"
	case 4:
		return g.exprOrSubqueryKind(r, depth-1, ctx, kindNumber) + " " + pick(r, []string{"+", "-", "*", "/", "%", "&", "|"}) + " " + g.exprOrSubqueryKind(r, depth-1, ctx, kindNumber)
	case 5:
		return g.functionExpressionKind(r, depth-1, ctx, expect)
	case 6:
		return g.ifExpression(r, depth-1, ctx)
	case 7:
		return g.caseWhenExpression(r, depth-1, ctx)
	case 8:
		return g.terminalExpressionKind(r, ctx, expect)
	default:
		return g.terminalExpressionKind(r, ctx, expect)
	}
}

func (g *Generator) terminalExpression(r *random.RNG, ctx *genCtx) string {
	return g.terminalExpressionKind(r, ctx, kindAny)
}

func (g *Generator) terminalExpressionKind(r *random.RNG, ctx *genCtx, expect valueKind) string {
	switch r.Intn(7) {
	case 0:
		return g.columnRefKind(r, expect)
	case 1:
		ctx.add("literal")
		return g.literalOfKind(r, expect)
	case 2:
		return g.columnRefKind(r, expect)
	case 3:
		return g.functionExpressionKind(r, 0, ctx, expect)
	default:
		return g.columnRefKind(r, expect)
	}
}

func (g *Generator) functionExpression(r *random.RNG, depth int, ctx *genCtx) string {
	return g.functionExpressionKind(r, depth, ctx, kindAny)
}

func (g *Generator) functionExpressionKind(r *random.RNG, depth int, ctx *genCtx, expect valueKind) string {
	ctx.add("function")
	if expect == kindNumber {
		switch r.Intn(5) {
		case 0:
			return "abs(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		case 1:
			return "ceil(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		case 2:
			return "floor(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		case 3:
			return "round(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		default:
			return "pow(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		}
	}
	if expect == kindString {
		switch r.Intn(4) {
		case 0:
			return "lower(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
		case 1:
			return "upper(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
		case 2:
			return "concat(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
		default:
			return "substr(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.unsignedInt(r) + ", " + g.unsignedInt(r) + ")"
		}
	}
	if expect == kindBool && chance(r, 50) {
		return "isnull(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindAny) + ")"
	}
	if expect == kindTime && chance(r, 60) {
		return pick(r, []string{"now()", "today()"})
	}

	switch r.Intn(12) {
	case 0:
		return g.standardFunctionCall(r, depth, ctx)
	case 1:
		ctx.add("star_func")
		return "count(*)"
	case 2:
		ctx.add("cols_func")
		return "cols(count(" + g.columnRefKind(r, kindNumber) + "), " + g.columnRefKind(r, kindAny) + ")"
	case 3:
		if chance(r, 50) {
			return "cast(" + g.commonExpression(r, min(depth, 1), ctx) + " as " + pick(r, []string{"int", "integer", "bigint", "float", "double", "bool", "timestamp", "varchar(16)", "binary(16)", "decimal(10,2)", "json"}) + ")"
		}
		return "cast(" + g.commonExpression(r, min(depth, 1), ctx) + " as " + pick(r, []string{"varchar", "binary", "nchar", "varbinary"}) + ")"
	case 4:
		return "trim(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case 5:
		return "trim(" + pick(r, []string{"both", "leading", "trailing"}) + " from " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case 6:
		return "trim(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + " from " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case 7:
		return "position(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + " in " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case 8:
		if chance(r, 50) {
			return pick(r, []string{"substr", "substring"}) + "(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.unsignedInt(r) + ", " + g.unsignedInt(r) + ")"
		}
		return pick(r, []string{"substr", "substring"}) + "(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + " from " + g.unsignedInt(r) + ")"
	case 9:
		return "replace(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case 10:
		return pick(r, []string{"now()", "today()", "timezone()", "database()", "client_version()", "server_version()", "server_status()", "current_user()", "user()", "pi()"})
	default:
		if chance(r, 50) {
			return "rand()"
		}
		return "rand(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
	}
}

func (g *Generator) standardFunctionCall(r *random.RNG, depth int, ctx *genCtx) string {
	fn := pick(r, g.funcNames)
	switch fn {
	case "abs", "ceil", "floor":
		return fn + "(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
	case "round":
		if chance(r, 70) {
			return "round(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
		}
		return "round(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ", " + g.unsignedInt(r) + ")"
	case "pow":
		return "pow(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
	case "length", "lower", "upper":
		return fn + "(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case "concat":
		return "concat(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ", " + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindString) + ")"
	case "sum", "avg", "min", "max":
		return fn + "(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
	default:
		return "abs(" + g.exprOrSubqueryKind(r, min(depth, 1), ctx, kindNumber) + ")"
	}
}

func (g *Generator) ifExpression(r *random.RNG, depth int, ctx *genCtx) string {
	switch r.Intn(6) {
	case 0:
		return "if(" + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ")"
	case 1:
		return "ifnull(" + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ")"
	case 2:
		return "nvl(" + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ")"
	case 3:
		return "nvl2(" + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ")"
	case 4:
		return "nullif(" + g.commonExpression(r, depth, ctx) + ", " + g.commonExpression(r, depth, ctx) + ")"
	default:
		return "coalesce(" + g.expressionList(r, depth, ctx) + ")"
	}
}

func (g *Generator) caseWhenExpression(r *random.RNG, depth int, ctx *genCtx) string {
	if chance(r, 50) {
		return "case when " + g.commonExpression(r, depth, ctx) + " then " + g.commonExpression(r, depth, ctx) + " else " + g.commonExpression(r, depth, ctx) + " end"
	}
	return "case " + g.commonExpression(r, depth, ctx) + " when " + g.commonExpression(r, depth, ctx) + " then " + g.commonExpression(r, depth, ctx) + " else " + g.commonExpression(r, depth, ctx) + " end"
}

func (g *Generator) expressionList(r *random.RNG, depth int, ctx *genCtx) string {
	n := 1 + r.Intn(3)
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, g.exprOrSubquery(r, depth, ctx))
	}
	return strings.Join(items, ", ")
}

func (g *Generator) insertQuery(r *random.RNG, depth int, ctx *genCtx) string {
	ctx.add("insert_query")
	dst, src := g.pickTwoTableNames(r)
	return "insert into " + dst + " select * from " + src + " limit 1"
}

func (g *Generator) fullTableName(r *random.RNG) string {
	return g.tableRefName(r)
}

func (g *Generator) columnRef(r *random.RNG) string {
	return g.columnRefKind(r, kindAny)
}

func (g *Generator) columnRefKind(r *random.RNG, kind valueKind) string {
	if g.typedCols != nil {
		pool := g.typedCols[kind]
		if len(pool) == 0 {
			pool = g.typedCols[kindAny]
		}
		if len(pool) > 0 {
			ref := pool[r.Intn(len(pool))]
			return ref.Name
		}
	}
	return g.columnName(r)
}

func (g *Generator) tableRefName(r *random.RNG) string {
	return pick(r, g.tables)
}

func (g *Generator) pickTwoTableNames(r *random.RNG) (string, string) {
	left := g.tableRefName(r)
	right := g.tableRefName(r)
	if left == "" {
		left = "t1"
	}
	if right == "" {
		right = "t2"
	}
	if len(g.tables) > 1 && right == left {
		for i := 0; i < 4 && right == left; i++ {
			right = g.tableRefName(r)
		}
		if right == left {
			for _, t := range g.tables {
				if t != left {
					right = t
					break
				}
			}
		}
	}
	return left, right
}

func (g *Generator) timeColumnForTable(table string) string {
	if g.tableCols != nil {
		if cols, ok := g.tableCols[table]; ok {
			for _, c := range cols {
				if c.Kind == kindTime {
					return c.Name
				}
			}
			if len(cols) > 0 {
				return cols[0].Name
			}
		}
	}
	if len(g.columns) > 0 {
		return g.columns[0]
	}
	return "ts"
}

func (g *Generator) timeJoinCondition(leftTable, rightTable string) string {
	return leftTable + "." + g.timeColumnForTable(leftTable) + " = " + rightTable + "." + g.timeColumnForTable(rightTable)
}

func (g *Generator) columnName(r *random.RNG) string {
	return pick(r, g.columns)
}

func (g *Generator) alias(r *random.RNG) string {
	return pick(r, g.aliases)
}

func (g *Generator) literal(r *random.RNG) string {
	return g.literalOfKind(r, kindAny)
}

func (g *Generator) literalOfKind(r *random.RNG, kind valueKind) string {
	switch kind {
	case kindBool:
		if chance(r, 50) {
			return "true"
		}
		return "false"
	case kindTime:
		return pick(r, []string{"now()", "today()", "timestamp '2024-01-01 00:00:00'"})
	case kindString:
		return "'s_" + fmt.Sprintf("%d", r.Intn(999)) + "'"
	case kindJSON:
		return "'{\"k\":1}'"
	case kindNumber:
		return fmt.Sprintf("%d", 1+r.Intn(100))
	}
	switch r.Intn(8) {
	case 0:
		return g.unsignedInt(r)
	case 1:
		return fmt.Sprintf("%d.%d", 1+r.Intn(9), r.Intn(99))
	case 2:
		return "'s_" + fmt.Sprintf("%d", r.Intn(999)) + "'"
	case 3:
		if chance(r, 50) {
			return "true"
		}
		return "false"
	case 4:
		return "timestamp '2024-01-01 00:00:00'"
	case 5:
		return g.unsignedInt(r)
	case 6:
		return "null"
	default:
		return g.unsignedInt(r)
	}
}

func (g *Generator) signedLiteral(r *random.RNG) string {
	switch r.Intn(8) {
	case 0:
		return fmt.Sprintf("%d", 1+r.Intn(100))
	case 1:
		return fmt.Sprintf("+%d", 1+r.Intn(100))
	case 2:
		return fmt.Sprintf("-%d", 1+r.Intn(100))
	case 3:
		return fmt.Sprintf("%d.%d", 1+r.Intn(10), r.Intn(99))
	case 4:
		return fmt.Sprintf("+%d.%d", 1+r.Intn(10), r.Intn(99))
	case 5:
		return fmt.Sprintf("-%d.%d", 1+r.Intn(10), r.Intn(99))
	case 6:
		return "'x'"
	default:
		return pick(r, []string{"true", "false", "null", "now()", "today()"})
	}
}

func (g *Generator) pickPredicateKind(r *random.RNG) valueKind {
	switch r.Intn(6) {
	case 0:
		return kindNumber
	case 1:
		return kindString
	case 2:
		return kindBool
	case 3:
		return kindTime
	case 4:
		return kindJSON
	default:
		return kindAny
	}
}

func (g *Generator) unsignedInt(r *random.RNG) string {
	return fmt.Sprintf("%d", 1+r.Intn(50))
}

func optional(r *random.RNG, piece string, prob int) string {
	if chance(r, prob) {
		return piece
	}
	return ""
}

func chance(r *random.RNG, p int) bool {
	if p <= 0 {
		return false
	}
	if p >= 100 {
		return true
	}
	return r.Intn(100) < p
}

func pick(r *random.RNG, arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[r.Intn(len(arr))]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
