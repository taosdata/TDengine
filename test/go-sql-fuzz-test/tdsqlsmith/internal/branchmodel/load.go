package branchmodel

// load.go reads the positive, negative, and write-only SQL corpus files from
// disk and assembles them into a Corpus.
//
// load.go 从磁盘读取正例、负例和只写 SQL 语料文件,并将它们组装为 Corpus。

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CorpusContent holds the raw text of the corpus files, used for in-memory loading.
//
// CorpusContent 保存语料文件的原始文本,用于内存中加载。
type CorpusContent struct {
	SelectBranchMatrix   string // contents of select_branch_matrix.tsv / select_branch_matrix.tsv 的内容
	SelectNestedMatrix   string // contents of select_nested_matrix.tsv / select_nested_matrix.tsv 的内容
	SelectBranchNegative string // contents of select_branch_negative.tsv / select_branch_negative.tsv 的内容
	WriteSQLCases        string // contents of write_sql_cases.txt / write_sql_cases.txt 的内容
}

// LoadCorpus reads the branch matrix, nested matrix, negative, and write corpus
// files from dir and assembles them into a Corpus.
//
// LoadCorpus 从 dir 读取分支矩阵、嵌套矩阵、负例和写入语料文件,
// 并将它们组装为 Corpus。
func LoadCorpus(dir string) (*Corpus, error) {
	pos, err := loadSelectBranch(filepath.Join(dir, "select_branch_matrix.tsv"))
	if err != nil {
		return nil, err
	}
	nested, err := loadSelectNested(filepath.Join(dir, "select_nested_matrix.tsv"))
	if err != nil {
		return nil, err
	}
	neg, err := loadSelectNegative(filepath.Join(dir, "select_branch_negative.tsv"))
	if err != nil {
		return nil, err
	}
	writeSQL, err := loadWriteCorpus(filepath.Join(dir, "write_sql_cases.txt"))
	if err != nil {
		return nil, err
	}

	return buildCorpus(pos, nested, neg, writeSQL)
}

// buildCorpus combines the base and nested positive cases with the negative and
// write cases into a Corpus, erroring if the positive or negative set is empty.
//
// buildCorpus 将基础正例和嵌套正例与负例、写入用例合并为一个 Corpus,
// 如果正例或负例集合为空则返回错误。
func buildCorpus(pos []PositiveCase, nested []PositiveCase, neg []NegativeCase, writeSQL []string) (*Corpus, error) {
	allPos := make([]PositiveCase, 0, len(pos)+len(nested))
	allPos = append(allPos, pos...)
	allPos = append(allPos, nested...)
	if len(allPos) == 0 {
		return nil, fmt.Errorf("empty positive branch corpus")
	}
	if len(neg) == 0 {
		return nil, fmt.Errorf("empty negative branch corpus")
	}
	return &Corpus{Positive: allPos, Negative: neg, WriteSQL: writeSQL}, nil
}

// loadSelectBranch opens the select-branch matrix TSV at path and parses it into
// positive cases.
//
// loadSelectBranch 打开 path 处的 select-branch 矩阵 TSV 文件,并将其解析为正例。
func loadSelectBranch(path string) ([]PositiveCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectBranchFromReader(f, path)
}

// loadSelectBranchFromReader parses the 5-column select-branch matrix from r
// (skipping the header) into positive cases sourced from select_branch_matrix.
//
// loadSelectBranchFromReader 从 r 解析 5 列的 select-branch 矩阵(跳过表头),
// 得到来源为 select_branch_matrix 的正例。
func loadSelectBranchFromReader(r io.Reader, source string) ([]PositiveCase, error) {
	sc := bufio.NewScanner(r)
	line := 0
	out := make([]PositiveCase, 0, 128)
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 5 {
			return nil, fmt.Errorf("invalid %s line %d", source, line)
		}
		out = append(out, PositiveCase{
			ID:        cols[0],
			Rule:      cols[1],
			BranchSig: cols[2],
			SQL:       cols[3],
			KeyAssert: cols[4],
			Source:    "select_branch_matrix",
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", source, err)
	}
	return out, nil
}

// loadSelectNested opens the select-nested matrix TSV at path and parses it into
// positive cases.
//
// loadSelectNested 打开 path 处的 select-nested 矩阵 TSV 文件,并将其解析为正例。
func loadSelectNested(path string) ([]PositiveCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectNestedFromReader(f, path)
}

// loadSelectNestedFromReader parses the 4-column select-nested matrix from r
// (skipping the header) into positive cases sourced from select_nested_matrix.
//
// loadSelectNestedFromReader 从 r 解析 4 列的 select-nested 矩阵(跳过表头),
// 得到来源为 select_nested_matrix 的正例。
func loadSelectNestedFromReader(r io.Reader, source string) ([]PositiveCase, error) {
	sc := bufio.NewScanner(r)
	line := 0
	out := make([]PositiveCase, 0, 64)
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			return nil, fmt.Errorf("invalid %s line %d", source, line)
		}
		out = append(out, PositiveCase{
			ID:        cols[0],
			Rule:      cols[1],
			BranchSig: cols[1],
			SQL:       cols[2],
			KeyAssert: cols[3],
			Source:    "select_nested_matrix",
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", source, err)
	}
	return out, nil
}

// loadSelectNegative opens the select-branch negative TSV at path and parses it
// into negative cases.
//
// loadSelectNegative 打开 path 处的 select-branch 负例 TSV 文件,并将其解析为负例。
func loadSelectNegative(path string) ([]NegativeCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectNegativeFromReader(f, path)
}

// loadSelectNegativeFromReader parses the 4-column negative corpus from r
// (skipping the header) into negative cases.
//
// loadSelectNegativeFromReader 从 r 解析 4 列的负例语料(跳过表头),得到负例。
func loadSelectNegativeFromReader(r io.Reader, source string) ([]NegativeCase, error) {
	sc := bufio.NewScanner(r)
	line := 0
	out := make([]NegativeCase, 0, 32)
	for sc.Scan() {
		line++
		s := sc.Text()
		if line == 1 {
			continue
		}
		cols := strings.Split(s, "\t")
		if len(cols) != 4 {
			return nil, fmt.Errorf("invalid %s line %d", source, line)
		}
		out = append(out, NegativeCase{
			ID:      cols[0],
			Rule:    cols[1],
			SQL:     cols[2],
			ErrType: cols[3],
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", source, err)
	}
	return out, nil
}

// loadWriteCorpus opens the write-SQL cases file at path and parses it into a
// list of statements.
//
// loadWriteCorpus 打开 path 处的 write-SQL 用例文件,并将其解析为语句列表。
func loadWriteCorpus(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadWriteCorpusFromReader(f, path)
}

// loadWriteCorpusFromReader reads write statements from r, skipping blank lines
// and lines beginning with '#'.
//
// loadWriteCorpusFromReader 从 r 读取写入语句,跳过空行和以 '#' 开头的行。
func loadWriteCorpusFromReader(r io.Reader, source string) ([]string, error) {
	sc := bufio.NewScanner(r)
	out := make([]string, 0, 512)
	for sc.Scan() {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", source, err)
	}
	return out, nil
}
