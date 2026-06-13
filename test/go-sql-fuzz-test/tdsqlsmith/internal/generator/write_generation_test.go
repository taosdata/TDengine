package generator

import (
	"testing"

	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/random"
	"tdsqlsmith/internal/util"
)

func loadCorpus(t *testing.T) *branchmodel.Corpus {
	t.Helper()
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir: %v", err)
	}
	corpus, err := branchmodel.LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	return corpus
}

func TestGeneratorCanProduceWriteStatements(t *testing.T) {
	corpus := loadCorpus(t)
	g, err := New(corpus.Positive, Config{MutationLevel: 1, WriteSQL: corpus.WriteSQL, WriteRatio: 100})
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}
	r := random.New(1)
	seenWrite := false
	for i := 0; i < 200; i++ {
		out, err := g.Next(r, nil)
		if err != nil {
			t.Fatalf("next failed: %v", err)
		}
		if out.Kind == "write" {
			seenWrite = true
			break
		}
	}
	if !seenWrite {
		t.Fatalf("expected write statement generation")
	}
}
