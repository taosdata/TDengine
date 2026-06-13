package sqlparser

import "testing"

func TestAlterDatabaseOptionChain(t *testing.T) {
	stmt, err := Parse("alter database db1 buffer 10 wal_retention_period -5 ss_keeplocal 2;")
	if err != nil {
		t.Fatalf("parse alter database option chain failed: %v", err)
	}
	s, ok := stmt.(*AlterDatabaseStmt)
	if !ok {
		t.Fatalf("expected *AlterDatabaseStmt, got %T", stmt)
	}
	if s.Name != "db1" || s.Options == nil {
		t.Fatalf("unexpected alter database stmt: %+v", s)
	}
	if s.Options.Buffer != 10 {
		t.Fatalf("unexpected buffer: %+v", s.Options)
	}
	if s.Options.WalRetentionPeriod != -5 || !s.Options.WalRetentionPeriodIsSet {
		t.Fatalf("unexpected wal_retention_period: %+v", s.Options)
	}
	if s.Options.SsKeepLocal != 2 {
		t.Fatalf("unexpected ss_keeplocal: %+v", s.Options)
	}
}
