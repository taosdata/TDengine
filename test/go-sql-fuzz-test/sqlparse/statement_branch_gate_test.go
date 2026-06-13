package sqlparser

import (
	"reflect"
	"sort"
	"testing"
)

func TestStatementBranchGate_RoundTripAndTypeCoverage(t *testing.T) {
	roundTripSQLs := gatherGlobalPositiveSQLFromTests(t)
	sqls := append([]string{}, roundTripSQLs...)
	sqls = append(sqls,
		"create bnode on dnode 1;",
		"drop bnode on dnode 1;",
		"create qnode on dnode 1;",
		"drop qnode on dnode 1;",
		"restore qnode on dnode 1;",
		"create mount if not exists m1 on dnode 1 from '/tmp/x';",
		"drop mount if exists m1;",
		"drop function if exists f1;",
		"insert into t values(1);",
		"delete from db1.t1;",
		"drop view if exists db1.v1;",
		"create index if not exists idx1 on db1.t1(v);",
		"create rsma if not exists r1 on db1.t1 function(avg(v)) interval(1d,2d);",
		"rollup database db1;",
		"drop tsma if exists db1.ts1;",
		"alter tsma if exists db1.ts1 function(avg);",
		"create table if not exists using db1.st1 () file 'f1';",
		"create vtable if not exists db1.vt2 using db1.st1 tags (1, 'x');",
		"alter rsma if exists db1.r1 function(avg(v),sum(v));",
		"create table if not exists db1.ta using db1.st1 tags(1) if not exists db1.tb using db1.st1 tags(2);",
		"create xnode 'n1';",
		"drop table if exists db1.t1;",
		"alter table db1.t1 add column c2 int;",
		"recalculate stream db1.s1 from 1 to 20200101;",
	)
	if len(sqls) == 0 {
		t.Fatalf("no sql collected for statement branch gate")
	}

	typeHit := map[string]int{}
	roundTripSet := map[string]struct{}{}
	for _, sql := range roundTripSQLs {
		roundTripSet[sql] = struct{}{}
	}

	for _, sql := range sqls {
		stmt, err := Parse(sql)
		if err != nil {
			continue
		}
		typ := reflect.TypeOf(stmt).String()
		typeHit[typ]++
		if _, ok := roundTripSet[sql]; ok {
			runStatementRoundTrip(t, sql)
		}
	}

	var missing []string
	for _, typ := range expectedStatementTypes() {
		if typeHit[typ] == 0 {
			missing = append(missing, typ)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("statement branch gate missing statement types: %v", missing)
	}

	// Branch-like minimum sample thresholds for high-fanout statements.
	branchMin := map[string]int{
		"*sqlparser.SelectStmt":      15,
		"*sqlparser.ShowStmt":        20,
		"*sqlparser.GrantStmt":       20,
		"*sqlparser.CreateTableStmt": 10,
	}
	for typ, min := range branchMin {
		if got := typeHit[typ]; got < min {
			t.Fatalf("statement branch gate low sample count for %s: got=%d want>=%d", typ, got, min)
		}
	}
}
