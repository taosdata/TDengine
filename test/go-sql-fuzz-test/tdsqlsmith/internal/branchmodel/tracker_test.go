package branchmodel

import (
	"strings"
	"testing"
	"time"

	"sqlparser"
	"tdsqlsmith/internal/util"
)

func TestTrackerHitsAllPositiveCasesFromCorpus(t *testing.T) {
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	corpus, err := LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus failed: %v", err)
	}
	tr := NewTracker(corpus.Positive, corpus.Negative)
	for _, tc := range corpus.Positive {
		stmt, err := sqlparser.Parse(strings.TrimSpace(tc.SQL))
		if err != nil {
			t.Fatalf("parse failed for %s: %v", tc.ID, err)
		}
		_ = tr.TryMarkPositive(stmt, tc.SQL, time.Now())
	}
	if !tr.IsPositiveCovered() {
		s := tr.Summary()
		t.Fatalf("positive coverage not complete: hit=%d required=%d missing=%v", s.Hit, s.Required, s.Missing)
	}
}
