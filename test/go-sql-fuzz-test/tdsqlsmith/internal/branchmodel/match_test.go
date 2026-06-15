package branchmodel

import (
	"strings"
	"testing"

	"sqlparser"
	"tdsqlsmith/internal/util"
)

func TestPositiveMatrixCasesMatchOwnKeyAssertions(t *testing.T) {
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	corpus, err := LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus failed: %v", err)
	}
	for _, tc := range corpus.Positive {
		t.Run(tc.ID, func(t *testing.T) {
			stmt, err := sqlparser.Parse(strings.TrimSpace(tc.SQL))
			if err != nil {
				t.Fatalf("parse failed: %v sql=%q", err, tc.SQL)
			}
			if err := MatchPositive(stmt, tc.KeyAssert); err != nil {
				t.Fatalf("key assert mismatch: %v sql=%q key=%q", err, tc.SQL, tc.KeyAssert)
			}
		})
	}
}

func TestNegativeMatrixCasesReject(t *testing.T) {
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		t.Fatalf("resolve corpus dir failed: %v", err)
	}
	corpus, err := LoadCorpus(dir)
	if err != nil {
		t.Fatalf("load corpus failed: %v", err)
	}
	for _, tc := range corpus.Negative {
		t.Run(tc.ID, func(t *testing.T) {
			if _, err := sqlparser.Parse(strings.TrimSpace(tc.SQL)); err == nil {
				t.Fatalf("expected parse reject for %q", tc.SQL)
			}
		})
	}
}
