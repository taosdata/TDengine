package queryrules

// seeds.go loads seed SQL from base lists and corpus files and builds a seed
// pool indexed by the query rules each statement exercises.
//
// seeds.go 从基础列表和语料文件加载种子 SQL,并构建一个按每条语句
// 所覆盖查询规则建立索引的种子池。

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"tdsqlsmith/internal/parsergate"
)

// SeedCase is a single seed SQL statement together with the query rules it hits.
//
// SeedCase 表示单条种子 SQL 语句及其命中的查询规则。
type SeedCase struct {
	SQL   string   // the seed SQL text / 种子 SQL 文本
	Rules []string // query rule names exercised by this statement / 该语句覆盖的查询规则名
}

// SeedPool holds seed cases and an index from rule name to the seed indices
// that exercise that rule.
//
// SeedPool 保存种子用例,以及从规则名到覆盖该规则的种子下标的索引。
type SeedPool struct {
	seeds  []SeedCase       // all seed cases in insertion order / 按插入顺序排列的所有种子用例
	byRule map[string][]int // rule name -> indices into seeds / 规则名 -> 在 seeds 中的下标
}

// LoadSeedSQL loads seed SQL statements, combining the provided base list with
// query statements drawn from the valid-SQL and statement-branch corpus files
// under corpusDir, de-duplicated and order-preserving.
//
// LoadSeedSQL 加载种子 SQL 语句,将提供的 base 列表与从 corpusDir 下的
// valid-SQL 和 statement-branch 语料文件中提取的查询语句合并,
// 去重并保持顺序。
func LoadSeedSQL(corpusDir string, base []string) ([]string, error) {
	validPath := filepath.Join(corpusDir, "valid_sql_cases.tsv")
	statementPath := filepath.Join(corpusDir, "statement_branch_matrix.tsv")
	return loadSeedSQL(base, func(add func(string)) error {
		if err := loadFromValidCorpus(validPath, add); err != nil {
			return err
		}
		if err := loadFromStatementMatrix(statementPath, add); err != nil {
			return err
		}
		return nil
	})
}

// loadSeedSQL collects seed SQL from base, then the optional loadCorpus callback,
// then the supplemental seeds, trimming blanks and removing duplicates while
// preserving first-seen order.
//
// loadSeedSQL 依次从 base、可选的 loadCorpus 回调、以及补充种子收集种子 SQL,
// 去除空白并删除重复项,同时保持首次出现的顺序。
func loadSeedSQL(base []string, loadCorpus func(add func(string)) error) ([]string, error) {
	ordered := make([]string, 0, len(base)+256)
	seen := make(map[string]struct{}, len(base)+256)
	add := func(sqlText string) {
		sqlText = strings.TrimSpace(sqlText)
		if sqlText == "" {
			return
		}
		if _, ok := seen[sqlText]; ok {
			return
		}
		seen[sqlText] = struct{}{}
		ordered = append(ordered, sqlText)
	}
	for _, sqlText := range base {
		add(sqlText)
	}
	if loadCorpus != nil {
		if err := loadCorpus(add); err != nil {
			return nil, err
		}
	}
	for _, sqlText := range supplementalQuerySeeds {
		add(sqlText)
	}
	return ordered, nil
}

// BuildSeedPool parses each SQL string, determines the query rules it exercises
// via the catalog, and builds a SeedPool indexed by rule. Statements that fail to
// parse or hit no query rules are skipped; an error is returned if none qualify.
//
// BuildSeedPool 解析每条 SQL 字符串,通过 catalog 确定它覆盖的查询规则,
// 并构建一个按规则建立索引的 SeedPool。无法解析或未命中任何查询规则的语句
// 将被跳过;若没有符合条件的语句则返回错误。
func BuildSeedPool(catalog *Catalog, sqls []string) (*SeedPool, error) {
	if catalog == nil {
		return nil, fmt.Errorf("nil catalog")
	}
	pool := &SeedPool{
		seeds:  make([]SeedCase, 0, len(sqls)),
		byRule: make(map[string][]int, len(catalog.required)),
	}
	for _, sqlText := range sqls {
		pg := parsergate.ParseWithRules(sqlText)
		if pg.Err != nil {
			continue
		}
		hitRules := catalog.QueryRulesFromReductions(pg.Rules)
		if len(hitRules) == 0 && catalog.IsQueryRule("insert_query") && looksLikeInsertQuery(sqlText) {
			hitRules = []string{"insert_query"}
		}
		if len(hitRules) == 0 {
			continue
		}
		idx := len(pool.seeds)
		pool.seeds = append(pool.seeds, SeedCase{SQL: sqlText, Rules: hitRules})
		for _, r := range hitRules {
			pool.byRule[r] = append(pool.byRule[r], idx)
		}
	}
	if len(pool.seeds) == 0 {
		return nil, fmt.Errorf("no query-rule seed cases built")
	}
	return pool, nil
}

// looksLikeInsertQuery reports whether sqlText is an INSERT ... SELECT statement.
//
// looksLikeInsertQuery 报告 sqlText 是否为 INSERT ... SELECT 语句。
func looksLikeInsertQuery(sqlText string) bool {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	return strings.HasPrefix(lower, "insert into") && strings.Contains(lower, " select ")
}

// loadFromValidCorpus opens the valid-SQL TSV at path and feeds its query
// statements to add.
//
// loadFromValidCorpus 打开 path 处的 valid-SQL TSV 文件,并将其查询语句
// 传给 add。
func loadFromValidCorpus(path string, add func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadFromValidCorpusReader(f, path, add)
}

// loadFromValidCorpusReader reads a tab-separated valid-SQL corpus from reader,
// using the header to locate the sql and stmt_type columns, and passes select /
// insert-query / query-looking statements to add.
//
// loadFromValidCorpusReader 从 reader 读取以制表符分隔的 valid-SQL 语料,
// 通过表头定位 sql 和 stmt_type 列,并将 select / insert-query /
// 形似查询的语句传给 add。
func loadFromValidCorpusReader(reader io.Reader, source string, add func(string)) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t'
	csvReader.FieldsPerRecord = -1
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("read %s header: %w", source, err)
	}
	idx := indexByName(header)
	sqlIdx, okSQL := idx["sql"]
	typeIdx, okType := idx["stmt_type"]
	if !okSQL || !okType {
		return fmt.Errorf("invalid header in %s", source)
	}
	for {
		row, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read %s: %w", source, err)
		}
		if sqlIdx >= len(row) || typeIdx >= len(row) {
			continue
		}
		sqlText := strings.TrimSpace(row[sqlIdx])
		stmtType := strings.TrimSpace(row[typeIdx])
		if sqlText == "" {
			continue
		}
		if stmtType == "SelectStmt" || stmtType == "InsertQueryStmt" || looksLikeQuery(sqlText) {
			add(sqlText)
		}
	}
	return nil
}

// loadFromStatementMatrix opens the statement-branch matrix TSV at path and
// feeds its query statements to add.
//
// loadFromStatementMatrix 打开 path 处的 statement-branch 矩阵 TSV 文件,
// 并将其查询语句传给 add。
func loadFromStatementMatrix(path string, add func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadFromStatementMatrixReader(f, path, add)
}

// loadFromStatementMatrixReader reads the tab-separated statement-branch matrix
// from r, skipping the header, and passes select / insert-query / query-looking
// statements (column 5, validated against the expected type in column 6) to add.
//
// loadFromStatementMatrixReader 从 r 读取以制表符分隔的 statement-branch 矩阵,
// 跳过表头,并将 select / insert-query / 形似查询的语句
// (第 5 列,按第 6 列的期望类型校验)传给 add。
func loadFromStatementMatrixReader(r io.Reader, source string, add func(string)) error {
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) < 6 {
			continue
		}
		sqlText := strings.TrimSpace(cols[4])
		expect := strings.TrimSpace(cols[5])
		if sqlText == "" {
			continue
		}
		if expect == "SelectStmt" || expect == "InsertQueryStmt" || looksLikeQuery(sqlText) {
			add(sqlText)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", source, err)
	}
	return nil
}

// looksLikeQuery reports whether sqlText appears to be a query: a SELECT, a
// parenthesized SELECT, an INSERT ... SELECT, or an EXPLAIN SELECT.
//
// looksLikeQuery 报告 sqlText 是否看起来像查询:SELECT、带括号的 SELECT、
// INSERT ... SELECT 或 EXPLAIN SELECT。
func looksLikeQuery(sqlText string) bool {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	if strings.HasPrefix(lower, "select ") {
		return true
	}
	if strings.HasPrefix(lower, "(select ") {
		return true
	}
	if strings.HasPrefix(lower, "insert into") && strings.Contains(lower, " select ") {
		return true
	}
	if strings.HasPrefix(lower, "explain select") {
		return true
	}
	return false
}

// indexByName maps each column header (lower-cased and trimmed) to its index.
//
// indexByName 将每个列表头(转为小写并去除空白)映射到其下标。
func indexByName(cols []string) map[string]int {
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[strings.ToLower(strings.TrimSpace(c))] = i
	}
	return m
}

// supplementalQuerySeeds is a built-in list of query statements always added to
// the seed set to ensure coverage of specific functions and clauses.
//
// supplementalQuerySeeds 是内置的查询语句列表,始终被加入种子集合,
// 以确保覆盖特定的函数和子句。
var supplementalQuerySeeds = []string{
	"select abs(v) from t1;",
	"select count(*) from t1;",
	"select count(v) from t1;",
	"select cols(count(v), v) from t1;",
	"select cols(count(*), v) from t1;",
	"select cols(sum(v), v, c1) from t1;",
	"select tbname from t1;",
	"select timezone();",
	"select rand(1);",
	"select substring(v, 1, 2) from t1;",
	"select cast(v as varchar) from t1;",
	"select v from t1 where v in (1.25);",
	"select trim(both from ' abc ');",
	"select a.v from (t1 a join t2 b on a.ts = b.ts);",
	"select v from t1 fill(value, 0);",
	"select v from t1 fill(linear);",
	"select v from t1 fill(prev);",
	"select v from t1 fill(prev, 1);",
	"select v from t1 fill(near);",
}
