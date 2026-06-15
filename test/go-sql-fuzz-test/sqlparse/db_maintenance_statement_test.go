package sqlparser

import "testing"

func TestSsMigrateDatabaseStatement_Parse(t *testing.T) {
	stmt, err := Parse("ssmigrate database db1;")
	if err != nil {
		t.Fatalf("parse ssmigrate database failed: %v", err)
	}
	s, ok := stmt.(*SsMigrateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *SsMigrateDatabaseStmt, got %T", stmt)
	}
	if s.DbName != "db1" {
		t.Fatalf("unexpected ssmigrate stmt: %+v", s)
	}
}

func TestTrimDatabaseWalStatement_Parse(t *testing.T) {
	stmt, err := Parse("trim database db1 wal;")
	if err != nil {
		t.Fatalf("parse trim database wal failed: %v", err)
	}
	s, ok := stmt.(*TrimDatabaseWalStmt)
	if !ok {
		t.Fatalf("expected *TrimDatabaseWalStmt, got %T", stmt)
	}
	if s.DbName != "db1" {
		t.Fatalf("unexpected trim wal stmt: %+v", s)
	}
}
