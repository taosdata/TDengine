package sqlparser

import "testing"

func TestSQLCoverage_DatabaseOptionErrorPaths(t *testing.T) {
	invalid := []string{
		"create database if not exists dbe1 buffer 999999999999999999999;",
		"create database if not exists dbe2 cachesize 999999999999999999999;",
		"create database if not exists dbe3 comp 1000;",
		"create database if not exists dbe4 days 999999999999999999999;",
		"create database if not exists dbe5 maxrows 999999999999999999999;",
		"create database if not exists dbe6 minrows 999999999999999999999;",
		"create database if not exists dbe7 pages 999999999999999999999;",
		"create database if not exists dbe8 pagesize 999999999999999999999;",
		"create database if not exists dbe9 tsdb_pagesize 999999999999999999999;",
		"create database if not exists dbe10 replica 1000;",
		"create database if not exists dbe11 wal_level 1000;",
		"create database if not exists dbe12 vgroups 999999999999999999999;",
		"create database if not exists dbe13 single_stable 1000;",
		"create database if not exists dbe14 retentions 1x:2d;",
		"create database if not exists dbe15 schemaless 1000;",
		"create database if not exists dbe16 wal_retention_period 999999999999999999999;",
		"create database if not exists dbe17 wal_retention_size 999999999999999999999;",
		"create database if not exists dbe18 wal_roll_period 999999999999999999999;",
		"create database if not exists dbe19 wal_segment_size 999999999999999999999;",
		"create database if not exists dbe20 stt_trigger 999999999999999999999;",
		"create database if not exists dbe21 table_prefix 999999999999999999999;",
		"create database if not exists dbe22 table_suffix 999999999999999999999;",
		"create database if not exists dbe23 ss_compact 1000;",
		"create database if not exists dbe24 ss_chunkpages 999999999999999999999;",
		"create database if not exists dbe25 ss_keeplocal 999999999999999999999;",
		"create database if not exists dbe26 keep_time_offset 999999999999999999999;",
		"create database if not exists dbe27 compact_interval 999999999999999999999;",
		"create database if not exists dbe28 compact_time_range x,2;",
		"create database if not exists dbe29 compact_time_offset 999999999999999999999;",
		"create database if not exists dbe30 is_audit 1000;",
		"create database if not exists dbe31 duration 1q;",
		"create database if not exists dbe32 keep 1q;",
		"create database if not exists dbe33 retentions 1x:2d;",
		"create database if not exists dbe34 retentions 1:2x;",
		"create database if not exists dbe35 wal_fsync_period x;",
		"create database if not exists dbe36 compact_time_range 1x,2;",
		"create database if not exists dbe37 compact_time_range 1,2x;",
		"create database if not exists dbe38 wal_fsync_period 999999999999999999999;",
		"create database if not exists dbe39 compact_time_range 999999999999999999999,2;",
		"create database if not exists dbe40 compact_time_range 2,999999999999999999999;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestSQLCoverage_ShowFormatMoreKinds(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"show databases;", "show databases"},
		{"show cluster alive;", "show cluster alive"},
		{"show create database db1;", "show create database db1"},
		{"show create table db1.t1;", "show create table db1.t1"},
		{"show create vtable db1.t1;", "show create vtable db1.t1"},
		{"show create stable db1.st1;", "show create stable db1.st1"},
		{"show create view db1.v1;", "show create view db1.v1"},
		{"show anodes;", "show anodes"},
		{"show anodes full;", "show anodes_full"},
		{"show cluster;", "show cluster"},
		{"show cluster machines;", "show cluster_machines"},
		{"show accounts;", "show accounts"},
		{"show child db1. tables like 't%';", "show child db1. tables like 't%'"},
		{"show db1. views like 'v%';", "show db1. views like 'v%'"},
		{"show db1. alive;", "show db1. alive"},
		{"show db1. disk_info;", "show db1. disk_info"},
		{"show db1. rsmas;", "show db1. rsmas"},
		{"show db1. retentions;", "show db1. retentions"},
		{"show db1. tsmas;", "show db1. tsmas"},
		{"show db1. vgroups;", "show db1. vgroups"},
		{"show tables;", "show tables"},
		{"show stables;", "show stables"},
		{"show vtables;", "show vtables"},
		{"show variables;", "show variables"},
		{"show tags from db1.t1;", "show tags from db1.t1"},
		{"show tags from t1;", "show tags from t1"},
		{"show indexes from t1;", "show indexes from t1"},
		{"show table tags tbname from t1;", "show table tags tbname from t1"},
		{"show scan 1;", "show scan 1"},
		{"show compact 1;", "show compact 1"},
		{"show retention 1;", "show retention 1"},
		{"show xnode worker;", "show xnode worker"},
		{"show vnodes;", "show vnodes"},
	}
	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.sql, err)
		}
		s, ok := stmt.(*ShowStmt)
		if !ok {
			t.Fatalf("expected *ShowStmt, got %T", stmt)
		}
		if got := formatStatementForRoundTrip(t, s); got != tc.want {
			t.Fatalf("unexpected show format for %q: got=%q want=%q", tc.sql, got, tc.want)
		}
	}
}

func TestSQLCoverage_GrantWildcardVariants(t *testing.T) {
	cases := []string{
		"grant read on table t1 to u1;",
		"grant read on * to u1;",
		"grant read on table * to u1;",
		"grant read on *.* to u1;",
		"grant read on table *.* to u1;",
		"grant read on db1.* to u1;",
		"grant read on table db1.* to u1;",
		"revoke read on table t1 from u1;",
		"revoke read on * from u1;",
		"revoke read on table * from u1;",
		"revoke read on *.* from u1;",
		"revoke read on table *.* from u1;",
		"revoke read on db1.* from u1;",
		"revoke read on table db1.* from u1;",
	}
	for _, sql := range cases {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
		if _, ok := stmt.(*GrantStmt); !ok {
			t.Fatalf("expected *GrantStmt for %q, got %T", sql, stmt)
		}
		_ = formatStatementForRoundTrip(t, stmt.(Statement))
	}
}

func TestSQLCoverage_InsertWalkUsingBranch(t *testing.T) {
	stmt, err := Parse("insert into db1.t1 using db1.st1 (tg1, tg2) tags (1, 'x') (ts, `A`) values (1, 2);")
	if err != nil {
		t.Fatalf("parse insert using failed: %v", err)
	}
	ins, ok := stmt.(InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	visited := 0
	if err := Walk(func(node SQLNode) (bool, error) {
		visited++
		return true, nil
	}, ins); err != nil {
		t.Fatalf("walk insert failed: %v", err)
	}
	if visited == 0 {
		t.Fatalf("expected walk to visit nodes")
	}
	if err := Walk(func(node SQLNode) (bool, error) {
		if _, ok := node.(*SQLVal); ok {
			return false, errWalk
		}
		return true, nil
	}, ins); err == nil {
		t.Fatalf("expected walk error for insert values")
	}
	stmt2, err := Parse("insert into t1 values (1);")
	if err != nil {
		t.Fatalf("parse insert simple failed: %v", err)
	}
	ins2, ok := stmt2.(InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt2)
	}
	if err := Walk(func(node SQLNode) (bool, error) {
		if _, ok := node.(TableName); ok {
			return false, errWalk
		}
		return true, nil
	}, ins2); err == nil {
		t.Fatalf("expected walk error for insert table name")
	}

	stmt3, err := Parse("insert into db1.t1 using db1.st1 tags (1) values (1);")
	if err != nil {
		t.Fatalf("parse insert using simple failed: %v", err)
	}
	ins3, ok := stmt3.(InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt3)
	}
	seenTable := 0
	if err := Walk(func(node SQLNode) (bool, error) {
		if _, ok := node.(TableName); ok {
			seenTable++
			if seenTable == 2 {
				return false, errWalk
			}
		}
		return true, nil
	}, ins3); err == nil {
		t.Fatalf("expected walk error for using table name")
	}
	_ = formatStatementForRoundTrip(t, ins)
}

var errWalk = &walkSentinelError{}

type walkSentinelError struct{}

func (*walkSentinelError) Error() string { return "walk sentinel" }

func TestSQLCoverage_CreateTableOptionErrorPaths(t *testing.T) {
	invalid := []string{
		"create table if not exists db1.te1 (ts timestamp) max_delay 1x;",
		"create table if not exists db1.te1b (ts timestamp) max_delay 1s,1x;",
		"create table if not exists db1.te2 (ts timestamp) watermark 1x;",
		"create table if not exists db1.te3 (ts timestamp) delete_mark 1x;",
		"create table if not exists db1.te4 (ts timestamp) keep x;",
		"create table if not exists db1.te4b (ts timestamp) keep xh;",
		"create table if not exists db1.te5 (ts timestamp) ttl 999999999999999999999;",
		"create stable if not exists db1.se1 (ts timestamp) tags (t int) virtual 2;",
		"create table if not exists db1.te6 (ts timestamp) max_delay 999999999999999999999a;",
		"create table if not exists db1.te7 (ts timestamp) max_delay 1s,999999999999999999999a;",
		"create table if not exists db1.te8 (ts timestamp) keep 999999999999999999999a;",
		"create stable if not exists db1.se2 (ts timestamp) tags (t int) virtual 999999999999999999999;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestSQLCoverage_CreateTableTagLoopAndDatabaseQuoting(t *testing.T) {
	stmt, err := Parse("create stable if not exists db1.st_cov (ts timestamp, v int) tags (t1 int, t2 binary(8));")
	if err != nil {
		t.Fatalf("parse create stable tags failed: %v", err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}
	if len(ct.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %+v", ct.Tags)
	}
	_ = formatStatementForRoundTrip(t, ct)

	stmt2, err := Parse("create database if not exists DbMixed buffer 1;")
	if err != nil {
		t.Fatalf("parse create database mixed case failed: %v", err)
	}
	cd, ok := stmt2.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt2)
	}
	if got := formatStatementForRoundTrip(t, cd); got != "create database if not exists dbmixed buffer 1" {
		t.Fatalf("unexpected mixed-case db format: %q", got)
	}
}

func TestSQLCoverage_CompactTimeRangeDurationParseErrors(t *testing.T) {
	invalid := []string{
		"create database if not exists dbe31 compact_time_range xh,2;",
		"create database if not exists dbe32 compact_time_range 2,xh;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestSQLCoverage_AlterDatabaseApplyErrorPath(t *testing.T) {
	invalid := []string{
		"alter database db1 buffer 999999999999999999999;",
		"alter database db1 compact_time_range 999999999999999999999,2;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestSQLCoverage_MultiCreateTableKeepFormatBranch(t *testing.T) {
	stmt, err := Parse("create table if not exists db1.ta using db1.st1 tags(1) keep 1d if not exists db1.tb using db1.st1 tags(2);")
	if err != nil {
		t.Fatalf("parse multi create with keep failed: %v", err)
	}
	m, ok := stmt.(*MultiCreateTableStmt)
	if !ok {
		t.Fatalf("expected *MultiCreateTableStmt, got %T", stmt)
	}
	if len(m.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %+v", m.Entries)
	}
	got := formatStatementForRoundTrip(t, m)
	if got == "" {
		t.Fatalf("expected non-empty format")
	}
}

func TestSQLCoverage_DatabaseRetentionAndDurationExtraBranches(t *testing.T) {
	valid := []string{
		"create database if not exists dbextra3 keep 1d,2d;",
	}
	for _, sql := range valid {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
		if _, ok := stmt.(*CreateDatabaseStmt); !ok {
			t.Fatalf("expected *CreateDatabaseStmt for %q, got %T", sql, stmt)
		}
		_ = formatStatementForRoundTrip(t, stmt.(Statement))
	}

	invalid := []string{
		"create database if not exists dbextra1 retentions -:1d;",
		"create database if not exists dbextra5 duration 123abc;",
		"create database if not exists dbextra6 keep 123abc;",
		"create database if not exists dbextra7 retentions 1d:a1;",
		"create database if not exists dbextra8 compact_time_range 1hh,2;",
		"create database if not exists dbextra9 compact_time_range 2,1hh;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}

func TestSQLCoverage_CreateTableKeepAndMaxDelayExtraBranches(t *testing.T) {
	invalid := []string{
		"create table if not exists db1.tmx1 (ts timestamp) max_delay 1hh;",
		"create table if not exists db1.tmx2 (ts timestamp) max_delay 1d,1hh;",
		"create table if not exists db1.tmx3 (ts timestamp) keep 1d,1hh;",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}
