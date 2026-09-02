package sqlparser

import (
	"bytes"
	"testing"
)

func TestShowStmt_FormatAndWalkCoverage(t *testing.T) {
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}

	var nilShow *ShowStmt
	nilShow.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil show format output: %q", got)
	}
	if err := nilShow.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil show walkSubtree failed: %v", err)
	}

	s := NewShowStmt("role_column_privileges")
	if s.Kind != "role_column_privileges" {
		t.Fatalf("unexpected kind: %+v", s)
	}
	s.iStatement()
	s.Format(tb)
	if got := tb.String(); got != "show role column privileges" {
		t.Fatalf("unexpected show format output: %q", got)
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("show walkSubtree failed: %v", err)
	}
}
