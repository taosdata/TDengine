package sqlparser

import "testing"

func TestCreateViewStatement_QueryBranch(t *testing.T) {
	stmt, err := Parse("create view v1 as select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	cv, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected *CreateViewStmt, got %T", stmt)
	}
	if cv.Name != "v1" || cv.Query == nil {
		t.Fatalf("unexpected create view stmt: %+v", cv)
	}

	stmt2, err := Parse("create view db1.v2 as select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	cv2, ok := stmt2.(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected *CreateViewStmt, got %T", stmt2)
	}
	if cv2.Name != "db1.v2" || cv2.Query == nil {
		t.Fatalf("unexpected create view stmt with db: %+v", cv2)
	}

	stmt3, err := Parse("create or replace view v3 as select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	cv3, ok := stmt3.(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected *CreateViewStmt, got %T", stmt3)
	}
	if !cv3.Replace || cv3.Name != "v3" || cv3.Query == nil {
		t.Fatalf("unexpected create or replace view stmt: %+v", cv3)
	}
}

func TestDescribeStatement_Branches(t *testing.T) {
	stmt, err := Parse("desc t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	d1, ok := stmt.(*DescribeStmt)
	if !ok || d1.Table != "t1" {
		t.Fatalf("unexpected desc stmt: %#v", stmt)
	}

	stmt2, err := Parse("describe db1.t2;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	d2, ok := stmt2.(*DescribeStmt)
	if !ok || d2.Table != "db1.t2" {
		t.Fatalf("unexpected describe stmt: %#v", stmt2)
	}
}

func TestExplainStatement_QueryBranch(t *testing.T) {
	stmt, err := Parse("explain select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ex, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("expected *ExplainStmt, got %T", stmt)
	}
	if ex.Target == nil {
		t.Fatalf("expected explain target")
	}
}

func TestExplainStatement_AnalyzeOptionsAndInsertQuery(t *testing.T) {
	stmt, err := Parse("explain analyze verbose true ratio 0.5 insert into t2 select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ex, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("expected *ExplainStmt, got %T", stmt)
	}
	if !ex.Analyze || !ex.Options.VerboseSet || !ex.Options.Verbose || !ex.Options.RatioSet || string(ex.Options.Ratio.Val.Bytes) != "0.5" {
		t.Fatalf("unexpected explain options: %+v", ex.Options)
	}
	ins, ok := ex.Target.(*InsertQueryStmt)
	if !ok || ins.Table != "t2" || ins.Query == nil {
		t.Fatalf("unexpected explain insert target: %#v", ex.Target)
	}
}

func TestKillStatements_Parse(t *testing.T) {
	cases := []struct {
		sql  string
		kind string
	}{
		{sql: "kill connection 1;", kind: "connection"},
		{sql: "kill transaction 2;", kind: "transaction"},
		{sql: "kill compact 3;", kind: "compact"},
		{sql: "kill retention 4;", kind: "retention"},
		{sql: "kill scan 5;", kind: "scan"},
		{sql: "kill ssmigrate 6;", kind: "ssmigrate"},
		{sql: "kill query 'qid';", kind: "query"},
	}

	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("parse %q failed: %v", tc.sql, err)
		}
		k, ok := stmt.(*KillStmt)
		if !ok {
			t.Fatalf("expected *KillStmt for %q, got %T", tc.sql, stmt)
		}
		if k.Kind != tc.kind {
			t.Fatalf("unexpected kill kind for %q: %+v", tc.sql, k)
		}
		if k.Target == "" {
			t.Fatalf("unexpected empty target for %q: %+v", tc.sql, k)
		}
	}
}

func TestFunctionBranches_ParityAdditions(t *testing.T) {
	cases := []string{
		"select replace(v, 'a', 'b') from t1;",
		"select substr(v, 1, 2) from t1;",
		"select substring(v from 1) from t1;",
		"select substring(v from 1 for 2) from t1;",
		"select rand() from t1;",
		"select rand(1) from t1;",
		"select cols(count(v), v) from t1;",
		"select cast(v as varchar(10)) from t1;",
		"select isnull(v) from t1;",
		"select isnotnull(v) from t1;",
		"select v in (1,2,3) from t1;",
		"select nvl2(v, 1, 0) from t1;",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err != nil {
			t.Fatalf("parse failed for %q: %v", sql, err)
		}
	}
}

func TestQuery_HintOptBranches(t *testing.T) {
	s1 := parseSelect(t, "select v from t1;")
	if s1.Hint != nil {
		t.Fatalf("expected nil hint, got %+v", s1.Hint)
	}

	s2 := parseSelect(t, "select /*+ batch_scan() */ v from t1;")
	if s2.Hint == nil || s2.Hint.HintType != HINT_BATCH_SCAN {
		t.Fatalf("expected batch_scan hint, got %+v", s2.Hint)
	}

	s3 := parseSelect(t, "select /*+ no_batch_scan() */ v from t1;")
	if s3.Hint == nil || s3.Hint.HintType != HINT_NO_BATCH_SCAN {
		t.Fatalf("expected no_batch_scan hint, got %+v", s3.Hint)
	}

	cases := []struct {
		sql  string
		kind HintType
	}{
		{"select /*+ sort_for_group() */ v from t1;", HINT_SORT_FOR_GROUP},
		{"select /*+ partition_first() */ v from t1;", HINT_PARTITION_FIRST},
		{"select /*+ para_tables_sort() */ v from t1;", HINT_PARA_TABLES_SORT},
		{"select /*+ smalldata_ts_sort() */ v from t1;", HINT_SMALLDATA_TS_SORT},
		{"select /*+ hash_join() */ v from t1;", HINT_HASH_JOIN},
		{"select /*+ skip_tsma() */ v from t1;", HINT_SKIP_TSMA},
		{"select /*+ win_optimize_batch() */ v from t1;", HINT_WIN_OPTIMIZE_BATCH},
		{"select /*+ win_optimize_single() */ v from t1;", HINT_WIN_OPTIMIZE_SINGLE},
	}
	for _, tc := range cases {
		s := parseSelect(t, tc.sql)
		if s.Hint == nil || s.Hint.HintType != tc.kind {
			t.Fatalf("sql=%q expected hint=%v, got %+v", tc.sql, tc.kind, s.Hint)
		}
	}

	s4 := parseSelect(t, "select /*+ batch_scan() hash_join() */ v from t1;")
	if s4.Hint == nil || s4.Hint.HintType != HINT_BATCH_SCAN {
		t.Fatalf("expected first hint batch_scan, got %+v", s4.Hint)
	}
}

func TestInsertQueryBranches(t *testing.T) {
	stmt, err := Parse("insert into t2 select v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ins, ok := stmt.(*InsertQueryStmt)
	if !ok {
		t.Fatalf("expected *InsertQueryStmt, got %T", stmt)
	}
	if ins.Table != "t2" || ins.Query == nil || len(ins.Columns) != 0 {
		t.Fatalf("unexpected insert query stmt: %+v", ins)
	}

	stmt2, err := Parse("insert into db1.t2(c1,c2) select v, v from t1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	ins2, ok := stmt2.(*InsertQueryStmt)
	if !ok {
		t.Fatalf("expected *InsertQueryStmt, got %T", stmt2)
	}
	if ins2.Table != "db1.t2" || ins2.Query == nil || len(ins2.Columns) != 2 {
		t.Fatalf("unexpected insert query stmt with cols: %+v", ins2)
	}
}
