package sqlparser

import "testing"

func TestQuery_EveryRejectsNonDurationLiteral(t *testing.T) {
	if _, err := Parse("select v from t1 every('10s');"); err == nil {
		t.Fatalf("expected parse error for string literal in every()")
	}
	if _, err := Parse("select v from t1 every(10);"); err == nil {
		t.Fatalf("expected parse error for integer literal in every()")
	}
}

func TestRecalculateStreamRangeRequired(t *testing.T) {
	if _, err := Parse("recalculate stream db1.s1;"); err == nil {
		t.Fatalf("expected parse error when recalculate range is missing")
	}
}

func TestUserOption_PasswordReuseUnlimitedRejected(t *testing.T) {
	if _, err := Parse("create user u_time2 pass 'p' password_reuse_time unlimited;"); err == nil {
		t.Fatalf("expected parse error for password_reuse_time unlimited")
	}
	if _, err := Parse("create user u_time3 pass 'p' password_reuse_max unlimited;"); err == nil {
		t.Fatalf("expected parse error for password_reuse_max unlimited")
	}
}

func TestCreateStableRequiresTags(t *testing.T) {
	_, err := Parse("create stable if not exists db1.st_no_tags (ts timestamp, v int);")
	if err == nil {
		t.Fatalf("expected parse error when create stable omits tags")
	}
}
