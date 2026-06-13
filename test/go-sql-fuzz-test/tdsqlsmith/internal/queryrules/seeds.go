package queryrules

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
	"tdsqlsmith/internal/random"
)

type SeedCase struct {
	SQL   string
	Rules []string
}

type SeedPool struct {
	seeds  []SeedCase
	byRule map[string][]int
}

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

func LoadSeedSQLFromContent(base []string, validSQLCases string, statementBranchMatrix string) ([]string, error) {
	return loadSeedSQL(base, func(add func(string)) error {
		if err := loadFromValidCorpusReader(strings.NewReader(validSQLCases), "embedded/valid_sql_cases.tsv", add); err != nil {
			return err
		}
		if err := loadFromStatementMatrixReader(strings.NewReader(statementBranchMatrix), "embedded/statement_branch_matrix.tsv", add); err != nil {
			return err
		}
		return nil
	})
}

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

func looksLikeInsertQuery(sqlText string) bool {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	return strings.HasPrefix(lower, "insert into") && strings.Contains(lower, " select ")
}

func (p *SeedPool) PickForMissing(rng *random.RNG, missingRules []string) (SeedCase, string, bool) {
	if p == nil || rng == nil || len(missingRules) == 0 {
		return SeedCase{}, "", false
	}
	candidates := make([]string, 0, len(missingRules))
	for _, r := range missingRules {
		if len(p.byRule[r]) == 0 {
			continue
		}
		candidates = append(candidates, r)
	}
	if len(candidates) == 0 {
		return SeedCase{}, "", false
	}
	rule := candidates[rng.Intn(len(candidates))]
	idxs := p.byRule[rule]
	if len(idxs) == 0 {
		return SeedCase{}, "", false
	}
	idx := idxs[rng.Intn(len(idxs))]
	if idx < 0 || idx >= len(p.seeds) {
		return SeedCase{}, "", false
	}
	return p.seeds[idx], rule, true
}

func loadFromValidCorpus(path string, add func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadFromValidCorpusReader(f, path, add)
}

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

func loadFromStatementMatrix(path string, add func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadFromStatementMatrixReader(f, path, add)
}

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

func indexByName(cols []string) map[string]int {
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[strings.ToLower(strings.TrimSpace(c))] = i
	}
	return m
}

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
