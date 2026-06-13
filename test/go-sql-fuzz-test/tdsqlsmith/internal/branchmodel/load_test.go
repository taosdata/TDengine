package branchmodel

import (
	"testing"

	"tdsqlsmith/internal/util"
)

func TestLoadCorpus(t *testing.T) {
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	corpus, err := LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus failed: %v", err)
	}
	if len(corpus.Positive) < 80 {
		t.Fatalf("expected >=80 positive cases, got %d", len(corpus.Positive))
	}
	if len(corpus.Negative) < 10 {
		t.Fatalf("expected >=10 negative cases, got %d", len(corpus.Negative))
	}
	if len(corpus.WriteSQL) < 50 {
		t.Fatalf("expected >=50 write cases, got %d", len(corpus.WriteSQL))
	}
}
