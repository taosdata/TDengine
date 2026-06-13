package sqlparser

import "testing"

func TestCreateFunctionStatement_ParseAndFields(t *testing.T) {
	stmt, err := Parse("create or replace aggregate function if not exists f1 as 'return 1' outputtype int bufsize 32 language 'python';")
	if err != nil {
		t.Fatalf("parse create function failed: %v", err)
	}
	s, ok := stmt.(*CreateFunctionStmt)
	if !ok {
		t.Fatalf("expected *CreateFunctionStmt, got %T", stmt)
	}
	if s.Name != "f1" || s.Body != "return 1" || s.OutputType != "int" || !s.IgnoreExists || !s.OrReplace || !s.Aggregate || s.Bufsize != 32 || s.Language != "python" {
		t.Fatalf("unexpected create function stmt: %+v", s)
	}
	got := formatStatementForRoundTrip(t, s)
	want := "create or replace aggregate function if not exists f1 as 'return 1' outputtype int bufsize 32 language 'python'"
	if got != want {
		t.Fatalf("unexpected create function format: got=%q want=%q", got, want)
	}
}

func TestCreateFunctionStatement_ErrorPath(t *testing.T) {
	if _, err := Parse("create function f1 as 'x' outputtype;"); err == nil {
		t.Fatalf("expected parse error for incomplete create function")
	}
}
