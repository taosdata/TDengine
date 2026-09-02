package sqlparser

import (
	"strings"
	"testing"
)

func TestErrorHandling_IncompleteSQL(t *testing.T) {
	_, err := Parse("select")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "incomplete sql") {
		t.Fatalf("expected incomplete sql error, got: %q", msg)
	}
	if !strings.Contains(msg, "position") {
		t.Fatalf("expected position in error, got: %q", msg)
	}
}

func TestErrorHandling_UnexpectedTokenNear(t *testing.T) {
	_, err := Parse("select from t")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "syntax error") {
		t.Fatalf("expected syntax error, got: %q", msg)
	}
	if !strings.Contains(msg, "near 'from'") {
		t.Fatalf("expected near token in error, got: %q", msg)
	}
}

func TestErrorHandling_InvalidCharNear(t *testing.T) {
	_, err := Parse("select @")
	if err == nil {
		t.Fatalf("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "near '@'") {
		t.Fatalf("expected invalid-char near token, got: %q", msg)
	}
}
