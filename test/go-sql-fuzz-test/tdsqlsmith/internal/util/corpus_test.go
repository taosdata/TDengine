package util

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCorpusDirProvidesRequiredFiles(t *testing.T) {
	t.Setenv("SQLPARSE_CORPUS_DIR", "")
	dir, err := ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}

	required := []string{
		"select_branch_matrix.tsv",
		"select_nested_matrix.tsv",
		"select_branch_negative.tsv",
		"write_sql_cases.txt",
		"valid_sql_cases.tsv",
		"statement_branch_matrix.tsv",
	}
	for _, name := range required {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing corpus file %s in %s: %v", name, dir, err)
		}
	}

	sqlparseRoot := filepath.Clean(filepath.Join(dir, "..", ".."))
	if _, err := os.Stat(filepath.Join(sqlparseRoot, "td_sql.y")); err != nil {
		t.Fatalf("missing grammar file under sqlparse root %s: %v", sqlparseRoot, err)
	}
	if _, err := os.Stat(filepath.Join(sqlparseRoot, "tool", "migrate", "query_rule_diff.sh")); err != nil {
		t.Fatalf("missing query rule script under sqlparse root %s: %v", sqlparseRoot, err)
	}
}
