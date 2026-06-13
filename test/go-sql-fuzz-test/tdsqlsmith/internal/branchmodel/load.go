package branchmodel

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type CorpusContent struct {
	SelectBranchMatrix   string
	SelectNestedMatrix   string
	SelectBranchNegative string
	WriteSQLCases        string
}

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

func LoadCorpusFromContent(content CorpusContent) (*Corpus, error) {
	pos, err := loadSelectBranchFromReader(strings.NewReader(content.SelectBranchMatrix), "embedded/select_branch_matrix.tsv")
	if err != nil {
		return nil, err
	}
	nested, err := loadSelectNestedFromReader(strings.NewReader(content.SelectNestedMatrix), "embedded/select_nested_matrix.tsv")
	if err != nil {
		return nil, err
	}
	neg, err := loadSelectNegativeFromReader(strings.NewReader(content.SelectBranchNegative), "embedded/select_branch_negative.tsv")
	if err != nil {
		return nil, err
	}
	writeSQL, err := loadWriteCorpusFromReader(strings.NewReader(content.WriteSQLCases), "embedded/write_sql_cases.txt")
	if err != nil {
		return nil, err
	}

	return buildCorpus(pos, nested, neg, writeSQL)
}

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

func loadSelectBranch(path string) ([]PositiveCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectBranchFromReader(f, path)
}

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

func loadSelectNested(path string) ([]PositiveCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectNestedFromReader(f, path)
}

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

func loadSelectNegative(path string) ([]NegativeCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadSelectNegativeFromReader(f, path)
}

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

func loadWriteCorpus(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return loadWriteCorpusFromReader(f, path)
}

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
