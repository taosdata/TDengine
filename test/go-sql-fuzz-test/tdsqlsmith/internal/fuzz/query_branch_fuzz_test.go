package fuzz

import (
	"strings"
	"testing"

	"sqlparser"
	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/util"
)

func seedCorpus(f *testing.F) {
	dir, err := util.ResolveCorpusDir("")
	if err != nil {
		f.Fatalf("resolve corpus dir failed: %v", err)
	}
	corpus, err := branchmodel.LoadCorpus(dir)
	if err != nil {
		f.Fatalf("load corpus failed: %v", err)
	}
	for _, tc := range corpus.Positive {
		f.Add(strings.TrimSpace(tc.SQL))
	}
	for _, tc := range corpus.Negative {
		f.Add(strings.TrimSpace(tc.SQL))
	}
}

func FuzzQueryBranchParseGate(f *testing.F) {
	seedCorpus(f)
	f.Fuzz(func(t *testing.T, sqlText string) {
		if strings.TrimSpace(sqlText) == "" {
			return
		}
		_ = parsergate.Parse(sqlText)
	})
}

func FuzzQueryBranchRoundTrip(f *testing.F) {
	seedCorpus(f)
	f.Fuzz(func(t *testing.T, sqlText string) {
		sqlText = strings.TrimSpace(sqlText)
		if sqlText == "" {
			return
		}
		res := parsergate.Parse(sqlText)
		if res.Err != nil {
			return
		}
		formatted := sqlparser.SQLNodeToString(res.Stmt)
		if strings.TrimSpace(formatted) == "" {
			t.Fatalf("empty formatted SQL from %q", sqlText)
		}
		res2 := parsergate.Parse(formatted)
		if res2.Err != nil {
			t.Fatalf("roundtrip parse failed: err=%v sql=%q formatted=%q", res2.Err, sqlText, formatted)
		}
	})
}
