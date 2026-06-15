// Package util provides helpers for locating the SQL corpus directory used by the fuzzer.
//
// Package util 提供用于定位 fuzzer 所用 SQL 语料库目录的辅助函数。
package util

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveCorpusDir returns the absolute path of the sqlparse SQL corpus directory.
// It tries the explicit input, the SQLPARSE_CORPUS_DIR environment variable, and
// several conventional locations relative to the working directory's parents.
//
// ResolveCorpusDir 返回 sqlparse SQL 语料库目录的绝对路径。
// 它依次尝试显式传入的 input、SQLPARSE_CORPUS_DIR 环境变量，
// 以及相对于工作目录各级父目录的若干约定位置。
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

// isCorpusDir reports whether dir contains the expected corpus marker TSV files.
//
// isCorpusDir 报告 dir 是否包含预期的语料库标记 TSV 文件。
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

// walkParents returns start and up to depth of its ancestor directories, stopping at the filesystem root.
//
// walkParents 返回 start 及其最多 depth 层祖先目录，到达文件系统根目录时停止。
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
