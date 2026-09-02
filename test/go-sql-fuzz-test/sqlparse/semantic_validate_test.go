package sqlparser

import (
	"strings"
	"testing"
)

func TestParseRejectsDoubleDashOutsideLiteral(t *testing.T) {
	_, err := Parse("select --v from t1;")
	if err == nil {
		t.Fatalf("expected semantic rejection for double dash")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
		t.Fatalf("expected semantic error, got: %v", err)
	}
}

func TestParseAllowsDoubleDashInsideStringLiteral(t *testing.T) {
	if _, err := Parse("select 'a--b' from t1;"); err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
}

func TestParseRejectsInvalidColsFunction(t *testing.T) {
	cases := []string{
		"select cols(abs(v), v) from t1;",
		"select cols(count(v), v as x) from t1;",
	}
	for _, sql := range cases {
		_, err := Parse(sql)
		if err == nil {
			t.Fatalf("expected semantic rejection for sql: %s", sql)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
			t.Fatalf("expected semantic error, got: %v", err)
		}
	}
}

func TestParseRejectsInvalidFunctionArity(t *testing.T) {
	cases := []string{
		"select rand(1, 2) from t1;",
		"select lower(a, b) from t1;",
	}
	for _, sql := range cases {
		_, err := Parse(sql)
		if err == nil {
			t.Fatalf("expected semantic rejection for sql: %s", sql)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
			t.Fatalf("expected semantic error, got: %v", err)
		}
	}
}

func TestParseRejectsGroupByMismatch(t *testing.T) {
	_, err := Parse("select b, c2 from t1 group by c2;")
	if err == nil {
		t.Fatalf("expected semantic rejection for group by mismatch")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "semantic") {
		t.Fatalf("expected semantic error, got: %v", err)
	}
}

func TestParseAllowsValidGroupedSelect(t *testing.T) {
	if _, err := Parse("select c2, count(*) from t1 group by c2;"); err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
}
