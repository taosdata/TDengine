// Package querygen builds random TDengine SQL statements as sqlparser ASTs and renders them to SQL text.
//
// Package querygen 构建随机的 TDengine SQL 语句为 sqlparser AST，并将其渲染为 SQL 文本。
package querygen

import (
	"fmt"
	"sort"
	"strings"

	"sqlparser"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/random"
)

// Config controls the recursion and breadth limits applied while generating a statement.
//
// Config 控制生成语句时应用的递归深度与广度限制。
type Config struct {
	MaxDepth       int // maximum nesting depth for query expressions and table references / 查询表达式和表引用的最大嵌套深度
	MaxSelectItems int // maximum number of items produced in a select list / select 列表中生成项的最大数量
	MaxExprDepth   int // maximum nesting depth for scalar expressions / 标量表达式的最大嵌套深度
}

// Column describes a single column of a table, with its name and SQL type text.
//
// Column 描述表中的单个列，包含其名称和 SQL 类型文本。
type Column struct {
	Name string // column name / 列名
	Type string // SQL type text, e.g. "int", "varchar(64)" / SQL 类型文本，例如 "int"、"varchar(64)"
}

// Table describes a table and the columns it exposes to the generator.
//
// Table 描述一张表以及它向生成器暴露的列。
type Table struct {
	Name    string   // table name / 表名
	Columns []Column // columns belonging to the table / 属于该表的列
}

// Schema is the set of tables the generator may reference.
//
// Schema 是生成器可以引用的表集合。
type Schema struct {
	Tables []Table // tables available for generation / 可用于生成的表
}

// valueKind is the coarse value category inferred for a column or expected by an expression.
//
// valueKind 是为列推断或由表达式期望的粗粒度值类别。
type valueKind int

const (
	kindAny    valueKind = iota // any/unspecified value kind / 任意/未指定的值类别
	kindNumber                  // numeric types (int, float, decimal, ...) / 数值类型（int、float、decimal 等）
	kindString                  // string/binary types (varchar, nchar, binary, ...) / 字符串/二进制类型（varchar、nchar、binary 等）
	kindBool                    // boolean type / 布尔类型
	kindTime                    // timestamp/date types / 时间戳/日期类型
	kindJSON                    // JSON type / JSON 类型
)

// columnRef is a resolved reference to a column, including its owning table and inferred kind.
//
// columnRef 是对列的已解析引用，包含其所属表和推断出的类别。
type columnRef struct {
	Table string    // owning table name / 所属表名
	Name  string    // column name / 列名
	Kind  valueKind // inferred value kind / 推断出的值类别
}

// Generated is the result of one generation attempt: the SQL text and the grammar tags it exercised.
//
// Generated 是一次生成尝试的结果：SQL 文本及其覆盖到的语法标签。
type Generated struct {
	SQL  string   // generated SQL statement text, terminated with ";" / 生成的 SQL 语句文本，以 ";" 结尾
	Tags []string // sorted grammar/feature tags touched during generation / 生成过程中覆盖到的、已排序的语法/特性标签
}

// Generator produces random SQL statements over a bound schema, drawing from pools of names and literals.
//
// Generator 在绑定的 schema 上生成随机 SQL 语句，从名称池和字面量池中取值。
type Generator struct {
	cfg         Config                    // generation limits / 生成限制
	tables      []string                  // available table names / 可用的表名
	columns     []string                  // union of all column names across tables / 所有表的列名并集
	typedCols   map[valueKind][]columnRef // columns grouped by inferred value kind / 按推断值类别分组的列
	tableCols   map[string][]columnRef    // columns grouped by owning table name / 按所属表名分组的列
	aliases     []string                  // candidate alias identifiers / 候选别名标识符
	funcNames   []string                  // candidate standard function names / 候选标准函数名
	pseudoCols  []string                  // candidate pseudo-column names (tbname, wstart, ...) / 候选伪列名（tbname、wstart 等）
	durationLit []string                  // candidate duration literals (1s, 5m, ...) / 候选时长字面量（1s、5m 等）
}

// genCtx accumulates the set of grammar tags exercised during a single generation pass.
//
// genCtx 累积单次生成过程中覆盖到的语法标签集合。
type genCtx struct {
	tags map[string]struct{} // set of tags touched, deduplicated / 覆盖到的标签集合，已去重
}

// add records a tag in the context, ignoring blank tags.
// add 在上下文中记录一个标签，忽略空白标签。
func (c *genCtx) add(tag string) {
	if strings.TrimSpace(tag) == "" {
		return
	}
	c.tags[tag] = struct{}{}
}

// list returns the recorded tags sorted alphabetically.
// list 返回按字母顺序排序的已记录标签。
func (c *genCtx) list() []string {
	out := make([]string, 0, len(c.tags))
	for k := range c.tags {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// DefaultConfig returns the default generation limits.
// DefaultConfig 返回默认的生成限制。
func DefaultConfig() Config {
	return Config{MaxDepth: 3, MaxSelectItems: 4, MaxExprDepth: 3}
}

// New creates a Generator, normalizing non-positive Config limits to defaults,
// seeding the default name/literal pools, and binding the default schema.
//
// New 创建一个 Generator，将非正的 Config 限制规整为默认值，
// 初始化默认的名称/字面量池，并绑定默认 schema。
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

// BindSchema replaces the generator's table/column pools from the given schema,
// inferring each column's value kind. It is a no-op when the schema yields no usable tables or columns.
//
// BindSchema 用给定的 schema 替换生成器的表/列池，并推断每个列的值类别。
// 当 schema 没有产生可用的表或列时，该方法为空操作。
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

// defaultSchema returns the built-in schema of three identical tables (t1, t2, t3)
// covering the common TDengine column types.
//
// defaultSchema 返回内置 schema，包含三张相同的表（t1、t2、t3），
// 覆盖常见的 TDengine 列类型。
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

// inferKind maps a SQL type text to a valueKind by case-insensitive substring matching.
// inferKind 通过大小写不敏感的子串匹配，将 SQL 类型文本映射为 valueKind。
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

// Next generates one random statement, retrying up to 16 times until the result parses cleanly.
// It returns the first statement that passes parsergate, or the last attempt if none parse.
// It errors when r is nil or no non-empty SQL could be produced.
//
// Next 生成一条随机语句，最多重试 16 次直到结果能够干净地解析。
// 它返回第一条通过 parsergate 的语句；若都无法解析，则返回最后一次尝试。
// 当 r 为 nil 或无法生成非空 SQL 时返回错误。
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

// queryExpressionAST builds a top-level query expression, optionally attaching ORDER BY,
// SLIMIT and LIMIT clauses.
//
// queryExpressionAST 构建顶层查询表达式，并可选地附加 ORDER BY、
// SLIMIT 和 LIMIT 子句。
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

// querySimpleAST builds a simple query, sometimes producing a UNION of two sub-queries when depth allows.
//
// querySimpleAST 构建一个简单查询，在深度允许时有时会生成两个子查询的 UNION。
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

// querySimpleOrSubqueryAST builds either a parenthesized subquery, a nested simple query,
// or a plain query specification depending on depth and chance.
//
// querySimpleOrSubqueryAST 根据深度和概率，构建带括号的子查询、嵌套的简单查询，
// 或普通的查询规约（query specification）。
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

// querySpecificationAST wraps querySpecificationASTPure, substituting a fallback SELECT when it yields nil.
//
// querySpecificationAST 封装 querySpecificationASTPure，当其返回 nil 时替换为兜底的 SELECT。
func (g *Generator) querySpecificationAST(r *random.RNG, depth int, ctx *genCtx) *sqlparser.SelectStmt {
	stmt := g.querySpecificationASTPure(r, depth, ctx)
	if stmt == nil {
		return fallbackSelectStmt()
	}
	return stmt
}

// fallbackSelectStmt returns a minimal valid "SELECT * FROM t1" statement.
// fallbackSelectStmt 返回一个最小的、合法的 "SELECT * FROM t1" 语句。
func fallbackSelectStmt() *sqlparser.SelectStmt {
	return &sqlparser.SelectStmt{
		Select: []sqlparser.Expr{&sqlparser.StarExpr{}},
		From:   &sqlparser.TableNameExpr{TableName: "t1"},
	}
}

// columnRefKind returns a column name of the requested kind, falling back to kindAny
// and then to a generic column name when no typed pool is available.
//
// columnRefKind 返回所请求类别的列名；当没有类型化的列池可用时，
// 先回退到 kindAny，再回退到通用列名。
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

// tableRefName returns a random table name from the pool.
// tableRefName 从表名池中返回一个随机表名。
func (g *Generator) tableRefName(r *random.RNG) string {
	return pick(r, g.tables)
}

// pickTwoTableNames returns two table names, preferring distinct names when more than one table exists.
//
// pickTwoTableNames 返回两个表名；当存在多于一张表时，优先返回不同的表名。
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

// timeColumnForTable returns a time-kind column for the table, falling back to its first column,
// then the first global column, then the literal "ts".
//
// timeColumnForTable 返回该表的时间类别列；依次回退到该表的第一个列、
// 全局第一个列，最后回退到字面量 "ts"。
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

// columnName returns a random column name from the pool.
// columnName 从列名池中返回一个随机列名。
func (g *Generator) columnName(r *random.RNG) string {
	return pick(r, g.columns)
}

// alias returns a random alias identifier from the pool.
// alias 从别名池中返回一个随机别名标识符。
func (g *Generator) alias(r *random.RNG) string {
	return pick(r, g.aliases)
}

// pickPredicateKind returns a randomly chosen value kind to type both sides of a predicate.
//
// pickPredicateKind 随机选择一个值类别，用于约束谓词两侧的类型。
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

// chance reports true with probability p percent (clamped to [0,100]).
// chance 以 p 百分比的概率返回 true（p 被限制在 [0,100]）。
func chance(r *random.RNG, p int) bool {
	if p <= 0 {
		return false
	}
	if p >= 100 {
		return true
	}
	return r.Intn(100) < p
}

// pick returns a random element of arr, or "" when arr is empty.
// pick 返回 arr 中的一个随机元素；当 arr 为空时返回 ""。
func pick(r *random.RNG, arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[r.Intn(len(arr))]
}

// min returns the smaller of a and b.
// min 返回 a 和 b 中较小的那个。
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
