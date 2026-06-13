package generator

import (
	"strings"
	"testing"

	"tdsqlsmith/internal/random"
)

func TestStructuredRewriteComparison(t *testing.T) {
	in := "select v from t1 where v > 1 limit 10"
	out, ok := rewriteComparison(in, random.New(1))
	if !ok {
		t.Fatalf("expected comparison rewrite")
	}
	if out == in {
		t.Fatalf("rewrite did not change sql")
	}
	if !strings.Contains(out, " where ") {
		t.Fatalf("unexpected rewrite output: %s", out)
	}
}

func TestStructuredRewriteUnion(t *testing.T) {
	in := "select v from t1 union all select v from t2"
	out, ok := rewriteUnion(in, random.New(2))
	if !ok {
		t.Fatalf("expected union rewrite")
	}
	if out == in {
		t.Fatalf("rewrite did not change sql")
	}
	if !strings.Contains(strings.ToLower(out), "union") {
		t.Fatalf("unexpected rewrite output: %s", out)
	}
}

func TestStructuredRewriteFill(t *testing.T) {
	in := "select v from t1 interval(10s) fill(prev)"
	out, ok := rewriteFillMode(in, random.New(3))
	if !ok {
		t.Fatalf("expected fill rewrite")
	}
	if out == in {
		t.Fatalf("rewrite did not change sql")
	}
	if !strings.Contains(strings.ToLower(out), "fill(") {
		t.Fatalf("unexpected rewrite output: %s", out)
	}
}

func TestMutateSQLLevel3CanUseStructuredRewrite(t *testing.T) {
	in := "select a.v from t1 a join t2 b on a.ts = b.ts where a.v > 1 union all select v from t3 limit 10"
	changed := false
	for i := 0; i < 64; i++ {
		out, mutated := mutateSQL(in, 3, random.New(uint64(500+i)))
		if !mutated {
			continue
		}
		if strings.TrimSpace(out) != strings.TrimSpace(in) {
			changed = true
			break
		}
	}
	if !changed {
		t.Fatalf("expected structured mutation to change sql")
	}
}
