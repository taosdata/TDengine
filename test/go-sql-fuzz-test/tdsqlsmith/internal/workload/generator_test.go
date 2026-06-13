package workload

import (
	"testing"

	"tdsqlsmith/internal/branchmodel"
	qgen "tdsqlsmith/internal/generator"
	"tdsqlsmith/internal/random"
	"tdsqlsmith/internal/util"
)

func loadCorpus(t *testing.T) *branchmodel.Corpus {
	t.Helper()
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus failed: %v", err)
	}
	corpus, err := branchmodel.LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus failed: %v", err)
	}
	return corpus
}

func TestBuildPools(t *testing.T) {
	corpus := loadCorpus(t)
	p := BuildPools(corpus.WriteSQL)
	if len(p.Pools[DMLInsert]) == 0 {
		t.Fatalf("expected insert pool")
	}
	if len(p.Pools[DMLDelete]) == 0 {
		t.Fatalf("expected delete pool")
	}
}

func TestWorkloadGeneratorNext(t *testing.T) {
	corpus := loadCorpus(t)
	q, err := qgen.New(corpus.Positive, qgen.Config{MutationLevel: 1})
	if err != nil {
		t.Fatalf("new query generator failed: %v", err)
	}
	w, err := New(DefaultConfig(), q, BuildPools(corpus.WriteSQL))
	if err != nil {
		t.Fatalf("new workload generator failed: %v", err)
	}
	r := random.New(1)
	for i := 0; i < 100; i++ {
		g, family, err := w.Next(r, []string{"SEL_001"})
		if err != nil {
			t.Fatalf("next failed: %v", err)
		}
		if family == "" || g.SQL == "" {
			t.Fatalf("invalid output family=%q generated=%+v", family, g)
		}
	}
}
