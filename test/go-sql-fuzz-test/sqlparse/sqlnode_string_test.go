package sqlparser

import "testing"

func TestSQLNodeToStringNil(t *testing.T) {
	if got := SQLNodeToString(nil); got != "" {
		t.Fatalf("expected empty string for nil node, got %q", got)
	}
}

func TestSQLNodeToStringRawExpr(t *testing.T) {
	if got := SQLNodeToString(&RawExpr{Name: "now"}); got != "now" {
		t.Fatalf("unexpected sql node rendering: %q", got)
	}
}
