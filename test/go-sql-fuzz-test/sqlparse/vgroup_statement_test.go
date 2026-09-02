package sqlparser

import (
	"bytes"
	"testing"
)

func TestBalanceVgroupStatement_Parse(t *testing.T) {
	stmt, err := Parse("balance vgroup;")
	if err != nil {
		t.Fatalf("parse balance vgroup failed: %v", err)
	}
	if _, ok := stmt.(*BalanceVgroupStmt); !ok {
		t.Fatalf("expected *BalanceVgroupStmt, got %T", stmt)
	}
}

func TestBalanceVgroupLeaderStatement_Parse(t *testing.T) {
	stmt, err := Parse("balance vgroup leader database db1;")
	if err != nil {
		t.Fatalf("parse balance vgroup leader database failed: %v", err)
	}
	s1, ok := stmt.(*BalanceVgroupLeaderStmt)
	if !ok {
		t.Fatalf("expected *BalanceVgroupLeaderStmt, got %T", stmt)
	}
	if s1.Database != "db1" || s1.VgroupID != -1 {
		t.Fatalf("unexpected leader by db stmt: %+v", s1)
	}

	stmt, err = Parse("balance vgroup leader on 11;")
	if err != nil {
		t.Fatalf("parse balance vgroup leader on id failed: %v", err)
	}
	s3, ok := stmt.(*BalanceVgroupLeaderStmt)
	if !ok {
		t.Fatalf("expected *BalanceVgroupLeaderStmt, got %T", stmt)
	}
	if s3.VgroupID != 11 || s3.Database != "" {
		t.Fatalf("unexpected leader by on id stmt: %+v", s3)
	}

	stmt, err = Parse("balance vgroup leader;")
	if err != nil {
		t.Fatalf("parse balance vgroup leader empty optional id failed: %v", err)
	}
	s4, ok := stmt.(*BalanceVgroupLeaderStmt)
	if !ok {
		t.Fatalf("expected *BalanceVgroupLeaderStmt, got %T", stmt)
	}
	if s4.VgroupID != -1 || s4.Database != "" {
		t.Fatalf("unexpected leader by empty optional id stmt: %+v", s4)
	}

	if _, err := Parse("balance vgroup leader 11;"); err == nil {
		t.Fatalf("expected parse error for missing ON keyword")
	}
}

func TestAssignLeaderStatement_Parse(t *testing.T) {
	stmt, err := Parse("assign leader force;")
	if err != nil {
		t.Fatalf("parse assign leader force failed: %v", err)
	}
	if _, ok := stmt.(*AssignLeaderStmt); !ok {
		t.Fatalf("expected *AssignLeaderStmt, got %T", stmt)
	}
}

func TestAlterVgroupKeepStatement_Parse(t *testing.T) {
	stmt, err := Parse("alter vgroup 7 set keep 3;")
	if err != nil {
		t.Fatalf("parse alter vgroup set keep failed: %v", err)
	}
	s, ok := stmt.(*AlterVgroupKeepStmt)
	if !ok {
		t.Fatalf("expected *AlterVgroupKeepStmt, got %T", stmt)
	}
	if s.VgroupID != 7 || s.Keep != 3 {
		t.Fatalf("unexpected alter vgroup keep stmt: %+v", s)
	}
}

func TestMergeVgroupStatement_Parse(t *testing.T) {
	stmt, err := Parse("merge vgroup 3 9;")
	if err != nil {
		t.Fatalf("parse merge vgroup failed: %v", err)
	}
	s, ok := stmt.(*MergeVgroupStmt)
	if !ok {
		t.Fatalf("expected *MergeVgroupStmt, got %T", stmt)
	}
	if s.SourceVgroupID != 3 || s.TargetVgroupID != 9 {
		t.Fatalf("unexpected merge vgroup stmt: %+v", s)
	}
}

func TestSplitVgroupStatement_Parse(t *testing.T) {
	stmt, err := Parse("split vgroup 8;")
	if err != nil {
		t.Fatalf("parse split vgroup failed: %v", err)
	}
	s1, ok := stmt.(*SplitVgroupStmt)
	if !ok {
		t.Fatalf("expected *SplitVgroupStmt, got %T", stmt)
	}
	if s1.VgroupID != 8 || s1.Force {
		t.Fatalf("unexpected split vgroup stmt: %+v", s1)
	}

	stmt, err = Parse("split vgroup 8 force;")
	if err != nil {
		t.Fatalf("parse split vgroup force failed: %v", err)
	}
	s2, ok := stmt.(*SplitVgroupStmt)
	if !ok {
		t.Fatalf("expected *SplitVgroupStmt, got %T", stmt)
	}
	if s2.VgroupID != 8 || !s2.Force {
		t.Fatalf("unexpected split vgroup force stmt: %+v", s2)
	}
}

func TestSplitVgroupStatement_ParseError(t *testing.T) {
	if _, err := Parse("split force vgroup 8;"); err == nil {
		t.Fatalf("expected parse error for invalid keyword order")
	}
}

func TestSplitVgroupStmt_FormatAndWalkCoverage(t *testing.T) {
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}

	var nilSplit *SplitVgroupStmt
	nilSplit.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil split format output: %q", got)
	}
	if err := nilSplit.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil split walkSubtree failed: %v", err)
	}

	s1 := NewSplitVgroupStmt(Token{Bytes: []byte("8")}, false)
	s1.iStatement()
	s1.Format(tb)
	if got := tb.String(); got != "split vgroup 8" {
		t.Fatalf("unexpected split format output: %q", got)
	}
	if err := s1.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("split walkSubtree failed: %v", err)
	}

	tb = &TrackedBuffer{Buffer: &bytes.Buffer{}}
	s2 := NewSplitVgroupStmt(Token{Bytes: []byte("8")}, true)
	s2.Format(tb)
	if got := tb.String(); got != "split vgroup 8 force" {
		t.Fatalf("unexpected split force format output: %q", got)
	}
}

func TestRedistributeVgroupStatement_Parse(t *testing.T) {
	stmt, err := Parse("redistribute vgroup 9 dnode 1 dnode 2;")
	if err != nil {
		t.Fatalf("parse redistribute vgroup failed: %v", err)
	}
	s, ok := stmt.(*RedistributeVgroupStmt)
	if !ok {
		t.Fatalf("expected *RedistributeVgroupStmt, got %T", stmt)
	}
	if s.VgroupID != 9 {
		t.Fatalf("unexpected redistribute vgroup id: %+v", s)
	}
	if len(s.DnodeIDs) != 2 || s.DnodeIDs[0] != 1 || s.DnodeIDs[1] != 2 {
		t.Fatalf("unexpected redistribute dnode ids: %+v", s)
	}
}

func TestRedistributeVgroupStatement_ParseError(t *testing.T) {
	if _, err := Parse("redistribute vgroup 9 dnode;"); err == nil {
		t.Fatalf("expected parse error for missing dnode id")
	}
}

func TestRedistributeVgroupStmt_FormatAndWalkCoverage(t *testing.T) {
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}

	var nilStmt *RedistributeVgroupStmt
	nilStmt.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil redistribute format output: %q", got)
	}
	if err := nilStmt.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil redistribute walkSubtree failed: %v", err)
	}

	s := NewRedistributeVgroupStmt(Token{Bytes: []byte("9")}, []Token{{Bytes: []byte("1")}, {Bytes: []byte("2")}})
	s.iStatement()
	s.Format(tb)
	if got := tb.String(); got != "redistribute vgroup 9 dnode 1 dnode 2" {
		t.Fatalf("unexpected redistribute format output: %q", got)
	}
	if err := s.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("redistribute walkSubtree failed: %v", err)
	}
}
