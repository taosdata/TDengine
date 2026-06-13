package generator

import (
	"testing"

	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/random"
	"tdsqlsmith/internal/util"
)

func loadPositiveCases(t *testing.T) []branchmodel.PositiveCase {
	t.Helper()
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir: %v", err)
	}
	corpus, err := branchmodel.LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	return corpus.Positive
}

func TestGeneratorDeterministic(t *testing.T) {
	cases := loadPositiveCases(t)
	g, err := New(cases, Config{MutationLevel: 2})
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}

	for i := 0; i < 100; i++ {
		seed := uint64(12345 + i)
		r1 := random.New(seed)
		r2 := random.New(seed)
		m := []string{cases[i%len(cases)].ID}
		a, err := g.Next(r1, m)
		if err != nil {
			t.Fatalf("next #1 failed: %v", err)
		}
		b, err := g.Next(r2, m)
		if err != nil {
			t.Fatalf("next #2 failed: %v", err)
		}
		if a.CaseID != b.CaseID || a.SQL != b.SQL || a.Mutated != b.Mutated {
			t.Fatalf("non-deterministic output seed=%d\na=%+v\nb=%+v", seed, a, b)
		}
	}
}

func TestGeneratorProducesNonEmptySQL(t *testing.T) {
	cases := loadPositiveCases(t)
	g, err := New(cases, Config{MutationLevel: 3})
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	r := random.New(20260216)
	for i := 0; i < 500; i++ {
		out, err := g.Next(r, nil)
		if err != nil {
			t.Fatalf("next failed at %d: %v", i, err)
		}
		if out.SQL == "" {
			t.Fatalf("empty sql at %d", i)
		}
	}
}
