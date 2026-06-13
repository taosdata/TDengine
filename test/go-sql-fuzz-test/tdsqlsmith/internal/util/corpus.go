package util

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolveCorpusDir(input string) (string, error) {
	candidates := []string{}
	if input != "" {
		candidates = append(candidates, input)
	}
	if env := os.Getenv("SQLPARSE_CORPUS_DIR"); env != "" {
		candidates = append(candidates, env)
	}

	wd, _ := os.Getwd()
	roots := walkParents(wd, 8)
	for _, root := range roots {
		candidates = append(candidates,
			filepath.Join(root, "../sqlparse/testdata/sql_corpus"),
			filepath.Join(root, "sqlparse/testdata/sql_corpus"),
			filepath.Join(root, "testdata/sql_corpus"),
		)
	}

	for _, raw := range candidates {
		abs, err := filepath.Abs(raw)
		if err != nil {
			continue
		}
		if isCorpusDir(abs) {
			return abs, nil
		}
	}
	return "", fmt.Errorf("cannot find sqlparse corpus dir; set SQLPARSE_CORPUS_DIR")
}

func isCorpusDir(dir string) bool {
	files := []string{
		"select_branch_matrix.tsv",
		"select_nested_matrix.tsv",
		"select_branch_negative.tsv",
	}
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return false
		}
	}
	return true
}

func walkParents(start string, depth int) []string {
	out := make([]string, 0, depth+1)
	cur := filepath.Clean(start)
	for i := 0; i <= depth; i++ {
		out = append(out, cur)
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	return out
}
