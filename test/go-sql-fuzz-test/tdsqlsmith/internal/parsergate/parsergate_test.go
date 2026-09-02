package parsergate

import "testing"

func TestParseClassifiesRejectTypes(t *testing.T) {
	ok := Parse("select v from t1;")
	if ok.Err != nil {
		t.Fatalf("unexpected parse error: %v", ok.Err)
	}

	bad := Parse("select from t1;")
	if bad.Err == nil {
		t.Fatalf("expected parse error")
	}
	if bad.ErrType == "" {
		t.Fatalf("expected non-empty parse err type")
	}
}

func TestParseQuery(t *testing.T) {
	res := Parse("select v from t1 where v > 1 order by v limit 1;")
	if res.Err != nil {
		t.Fatalf("unexpected parse error: %v", res.Err)
	}
	if len(res.Rules) != 0 {
		t.Fatalf("expected no reduce-rule trace, got: %v", res.Rules)
	}
}

func TestParseWithRulesQuery(t *testing.T) {
	res := ParseWithRules("select v from t1 where v > 1 order by v limit 1;")
	if res.Err != nil {
		t.Fatalf("unexpected parse error: %v", res.Err)
	}
	if len(res.Rules) == 0 {
		t.Fatalf("expected reduce-rule trace, got empty")
	}
}

func TestParseClassifiesSemanticReject(t *testing.T) {
	res := Parse("select rand(1, 2) from t1;")
	if res.Err == nil {
		t.Fatalf("expected semantic parse reject")
	}
	if res.ErrType != "semantic" {
		t.Fatalf("expected semantic err type, got: %s (%v)", res.ErrType, res.Err)
	}
}
