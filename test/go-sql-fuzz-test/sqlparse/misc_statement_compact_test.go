package sqlparser

import "testing"

func TestResetQueryCacheStatement(t *testing.T) {
	stmt, err := Parse("reset query cache;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if _, ok := stmt.(*ResetQueryCacheStmt); !ok {
		t.Fatalf("expected *ResetQueryCacheStmt, got %T", stmt)
	}
}

func TestFlushDatabaseStatement_Parse(t *testing.T) {
	stmt, err := Parse("flush database db1;")
	if err != nil {
		t.Fatalf("parse flush database failed: %v", err)
	}
	fd, ok := stmt.(*FlushDatabaseStmt)
	if !ok {
		t.Fatalf("expected *FlushDatabaseStmt, got %T", stmt)
	}
	if fd.DbName != "db1" {
		t.Fatalf("unexpected flush db stmt: %+v", fd)
	}
}

func TestTrimDatabaseStatement_Parse(t *testing.T) {
	stmt, err := Parse("trim database db1;")
	if err != nil {
		t.Fatalf("parse trim database failed: %v", err)
	}
	s1, ok := stmt.(*TrimDatabaseStmt)
	if !ok {
		t.Fatalf("expected *TrimDatabaseStmt, got %T", stmt)
	}
	if s1.DbName != "db1" || s1.BwLimit != 0 {
		t.Fatalf("unexpected trim database stmt: %+v", s1)
	}

	stmt, err = Parse("trim database db1 bwlimit 10;")
	if err != nil {
		t.Fatalf("parse trim database bwlimit failed: %v", err)
	}
	s2, ok := stmt.(*TrimDatabaseStmt)
	if !ok {
		t.Fatalf("expected *TrimDatabaseStmt, got %T", stmt)
	}
	if s2.DbName != "db1" || s2.BwLimit != 10 {
		t.Fatalf("unexpected trim database bwlimit stmt: %+v", s2)
	}
}

func TestRecalculateStreamRangeStatement(t *testing.T) {
	stmt, err := Parse("recalculate stream db1.s1 from 1 to 20200101;")
	if err != nil {
		t.Fatalf("parse recalculate stream range failed: %v", err)
	}
	s, ok := stmt.(*StreamStmt)
	if !ok {
		t.Fatalf("expected *StreamStmt, got %T", stmt)
	}
	if s.Action != "recalculate" || len(s.Names) != 1 || s.Names[0] != "db1.s1" || s.RecalcFrom != "1" || s.RecalcTo != "20200101" {
		t.Fatalf("unexpected recalculate stream stmt: %+v", s)
	}
}

func TestCreateStreamStatement_Parse(t *testing.T) {
	stmt, err := Parse("create stream if not exists s1 session(ts, 10s) into db1.tout (c1) tags(tag1 int as v) as select v from t1;")
	if err != nil {
		t.Fatalf("parse create stream failed: %v", err)
	}
	s, ok := stmt.(*StreamStmt)
	if !ok {
		t.Fatalf("expected *StreamStmt, got %T", stmt)
	}
	if s.Action != "create" || !s.NotExists || len(s.Names) != 1 || s.Names[0] != "s1" || s.Trigger == "" || s.OutTable == "" || s.Query == nil {
		t.Fatalf("unexpected create stream stmt: %+v", s)
	}
}

func TestCreateStreamStatement_FullStreamNameParse(t *testing.T) {
	stmt, err := Parse("create stream if not exists db1.s1 session(ts, 10s) into db1.tout (c1) as select v from t1;")
	if err != nil {
		t.Fatalf("parse create stream full name failed: %v", err)
	}
	s, ok := stmt.(*StreamStmt)
	if !ok {
		t.Fatalf("expected *StreamStmt, got %T", stmt)
	}
	if len(s.Names) != 1 || s.Names[0] != "db1.s1" {
		t.Fatalf("unexpected full stream name: %+v", s)
	}
}

func TestDropStreamStatement_FullStreamNameListParse(t *testing.T) {
	stmt, err := Parse("drop stream if exists db1.s1, db2.s2;")
	if err != nil {
		t.Fatalf("parse drop stream full name list failed: %v", err)
	}
	s, ok := stmt.(*StreamStmt)
	if !ok {
		t.Fatalf("expected *StreamStmt, got %T", stmt)
	}
	if s.Action != "drop" || !s.IfExists || len(s.Names) != 2 || s.Names[0] != "db1.s1" || s.Names[1] != "db2.s2" {
		t.Fatalf("unexpected drop stream stmt: %+v", s)
	}
}

func TestTopicStatement_QueryBranches(t *testing.T) {
	stmt, err := Parse("create topic if not exists tp1 as select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ct, ok := stmt.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt)
	}
	if ct.Reload || !ct.NotExists || ct.Name != "tp1" || ct.Query == nil {
		t.Fatalf("unexpected create topic stmt: %+v", ct)
	}

	stmt2, err := Parse("reload topic if exists tp1 as select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	rt, ok := stmt2.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt2)
	}
	if !rt.Reload || !rt.IfExists || rt.Name != "tp1" || rt.Query == nil {
		t.Fatalf("unexpected reload topic stmt: %+v", rt)
	}

	stmt3, err := Parse("create topic if not exists tp2 with meta as database db1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ct2, ok := stmt3.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt3)
	}
	if ct2.Reload || !ct2.NotExists || ct2.Name != "tp2" || ct2.MetaMode != "with_meta_as" || ct2.Database != "db1" {
		t.Fatalf("unexpected create topic database stmt: %+v", ct2)
	}

	stmt4, err := Parse("create topic if not exists tp3 only meta as stable db1.stb where v > 1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ct3, ok := stmt4.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt4)
	}
	if ct3.Reload || !ct3.NotExists || ct3.Name != "tp3" || ct3.MetaMode != "only_meta_as" || ct3.Stable != "db1.stb" || ct3.Where == nil {
		t.Fatalf("unexpected create topic stable stmt: %+v", ct3)
	}

	stmt5, err := Parse("drop topic if exists force tp3;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	dt, ok := stmt5.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt5)
	}
	if !dt.Drop || !dt.ExistsOpt || !dt.Force || dt.Name != "tp3" {
		t.Fatalf("unexpected drop topic stmt: %+v", dt)
	}

	stmt6, err := Parse("drop consumer group if exists force cg1 on tp3;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	dcg, ok := stmt6.(*TopicStmt)
	if !ok {
		t.Fatalf("expected *TopicStmt, got %T", stmt6)
	}
	if !dcg.DropGroup || !dcg.ExistsOpt || !dcg.Force || dcg.GroupName != "cg1" || dcg.OnTopic != "tp3" {
		t.Fatalf("unexpected drop consumer group stmt: %+v", dcg)
	}
}

func TestScanVgroupsWithDBNameCond(t *testing.T) {
	stmt, err := Parse("scan db1. vgroups in (1) start with 1 end with 2;")
	if err != nil {
		t.Fatalf("parse scan vgroups with db_name_cond failed: %v", err)
	}
	s, ok := stmt.(*ScanStmt)
	if !ok {
		t.Fatalf("expected *ScanStmt, got %T", stmt)
	}
	if s.Scope != "vgroups" || s.Name != "db1" || s.Start != "1" || s.End != "2" {
		t.Fatalf("unexpected scan stmt: %+v", s)
	}
}

func TestCompactVgroupsWithDBNameCond(t *testing.T) {
	stmt, err := Parse("compact db1. vgroups in (1) start with 1 end with 2 meta_only force;")
	if err != nil {
		t.Fatalf("parse compact vgroups with db_name_cond failed: %v", err)
	}
	s, ok := stmt.(*CompactStmt)
	if !ok {
		t.Fatalf("expected *CompactStmt, got %T", stmt)
	}
	if s.Scope != "vgroups" || s.Name != "db1" || s.Start != "1" || s.End != "2" || !s.MetaOnly || !s.Force {
		t.Fatalf("unexpected compact stmt: %+v", s)
	}
}
