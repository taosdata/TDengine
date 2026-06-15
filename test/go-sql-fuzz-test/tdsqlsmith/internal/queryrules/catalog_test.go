package queryrules

import (
	"path/filepath"
	"testing"

	"tdsqlsmith/internal/util"
)

func TestLoadCatalog(t *testing.T) {
	corpusDir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	sqlparseRoot := filepath.Clean(filepath.Join(corpusDir, "..", ".."))
	cat, err := LoadCatalog(sqlparseRoot)
	if err != nil {
		t.Fatalf("load catalog failed: %v", err)
	}
	if len(cat.RequiredRules()) < 100 {
		t.Fatalf("expected >=100 query rules, got %d", len(cat.RequiredRules()))
	}
}

func TestBuildSeedPool(t *testing.T) {
	corpusDir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	sqlparseRoot := filepath.Clean(filepath.Join(corpusDir, "..", ".."))
	cat, err := LoadCatalog(sqlparseRoot)
	if err != nil {
		t.Fatalf("load catalog failed: %v", err)
	}
	seedSQL, err := LoadSeedSQL(corpusDir, []string{"select v from t1;", "select count(*) from t1;"})
	if err != nil {
		t.Fatalf("load seed sql failed: %v", err)
	}
	pool, err := BuildSeedPool(cat, seedSQL)
	if err != nil {
		t.Fatalf("build seed pool failed: %v", err)
	}
	if len(pool.seeds) == 0 {
		t.Fatalf("expected non-empty seed pool")
	}
	for rule, idxs := range pool.byRule {
		for _, idx := range idxs {
			if idx < 0 || idx >= len(pool.seeds) {
				t.Fatalf("rule %s has out-of-range seed index %d", rule, idx)
			}
			seed := pool.seeds[idx]
			hit := false
			for _, r := range seed.Rules {
				if r == rule {
					hit = true
					break
				}
			}
			if !hit {
				t.Fatalf("rule %s indexed seed %d without matching rules: %v", rule, idx, seed.Rules)
			}
		}
	}
}
