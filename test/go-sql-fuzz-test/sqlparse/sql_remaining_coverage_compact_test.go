package sqlparser

import (
	"strings"
	"testing"
)

func TestSQLCoverage_GrantWithConditionVariants(t *testing.T) {
	cases := []struct {
		sql   string
		optr  int8
		objTy int16
	}{
		{"grant read on database db1 with v > 1 to u1;", 0, PRIV_OBJ_DB},
		{"grant read on table db1.t1 with v > 1 to u1;", 0, PRIV_OBJ_TBL},
		{"grant read with v > 1 to u1;", 0, PRIV_OBJ_CLUSTER},
		{"revoke read on database db1 with v > 1 from u1;", 1, PRIV_OBJ_DB},
		{"revoke read on table db1.t1 with v > 1 from u1;", 1, PRIV_OBJ_TBL},
		{"revoke read with v > 1 from u1;", 1, PRIV_OBJ_CLUSTER},
	}
	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.sql, err)
		}
		g, ok := stmt.(*GrantStmt)
		if !ok {
			t.Fatalf("expected *GrantStmt, got %T", stmt)
		}
		if g.OptrType != tc.optr || g.Privileges.ObjType != tc.objTy || g.Cond == nil {
			t.Fatalf("unexpected grant stmt for %q: %+v", tc.sql, g)
		}
		formatted := formatStatementForRoundTrip(t, g)
		if !strings.Contains(formatted, " with ") {
			t.Fatalf("expected formatted grant with condition, got %q", formatted)
		}
	}
}

func TestSQLCoverage_AlterTokenFormatBranches(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"alter token tk1 enable 1;", "alter token tk1 enable 1"},
		{"alter token tk1 enable 0;", "alter token tk1 enable 0"},
		{"alter token tk1 ttl 3;", "alter token tk1 ttl 3"},
		{"alter token tk1 provider 'p';", "alter token tk1 provider 'p'"},
		{"alter token tk1 extra_info 'x';", "alter token tk1 extra_info 'x'"},
	}
	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.sql, err)
		}
		a, ok := stmt.(*AlterTokenStmt)
		if !ok {
			t.Fatalf("expected *AlterTokenStmt, got %T", stmt)
		}
		if got := formatStatementForRoundTrip(t, a); got != tc.want {
			t.Fatalf("unexpected alter token format for %q: got=%q want=%q", tc.sql, got, tc.want)
		}
	}
}

func TestSQLCoverage_CreateBnodeProtocolBranches(t *testing.T) {
	stmt1, err := Parse("create bnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse create bnode failed: %v", err)
	}
	b1, ok := stmt1.(*CreateBnodeStmt)
	if !ok {
		t.Fatalf("expected *CreateBnodeStmt, got %T", stmt1)
	}
	if b1.Options.ProtoStr != "" {
		t.Fatalf("expected empty protocol by default: %+v", b1)
	}
	if got := formatStatementForRoundTrip(t, b1); got != "create bnode on dnode 1" {
		t.Fatalf("unexpected create bnode format without protocol: %q", got)
	}

	stmt2, err := Parse("create bnode on dnode 1 PROTOCOL 'grpc';")
	if err != nil {
		t.Fatalf("parse create bnode with protocol failed: %v", err)
	}
	b2, ok := stmt2.(*CreateBnodeStmt)
	if !ok {
		t.Fatalf("expected *CreateBnodeStmt, got %T", stmt2)
	}
	// Current parser behavior for bare identifier PROTOCOL: option key is normalized and
	// does not match the exact-case branch in SetBnodeOption.
	if b2.Options.ProtoStr != "" {
		t.Fatalf("expected empty protocol with current parser behavior, got %+v", b2)
	}
	if got := formatStatementForRoundTrip(t, b2); got != "create bnode on dnode 1" {
		t.Fatalf("unexpected create bnode format with protocol input: %q", got)
	}

	stmt3, err := Parse("create bnode on dnode 1 `PROTOCOL` 'grpc';")
	if err != nil {
		t.Fatalf("parse create bnode with quoted protocol key failed: %v", err)
	}
	b3, ok := stmt3.(*CreateBnodeStmt)
	if !ok {
		t.Fatalf("expected *CreateBnodeStmt, got %T", stmt3)
	}
	if strings.ToLower(b3.Options.ProtoStr) != "grpc" {
		t.Fatalf("expected protocol=grpc with quoted key, got %+v", b3)
	}
	if got := formatStatementForRoundTrip(t, b3); !strings.Contains(got, "protocol grpc") {
		t.Fatalf("unexpected create bnode format with quoted protocol: %q", got)
	}
}

func TestSQLCoverage_InsertIdentifierFormatting(t *testing.T) {
	stmt, err := Parse("insert into t1 (`A`, `b-c`) values(1,2);")
	if err != nil {
		t.Fatalf("parse insert with quoted identifiers failed: %v", err)
	}
	ins, ok := stmt.(InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if len(ins) != 1 || len(ins[0].Fields) != 2 {
		t.Fatalf("unexpected insert fields: %+v", ins)
	}
	formatted := formatStatementForRoundTrip(t, ins)
	if !strings.Contains(formatted, "`A`") || !strings.Contains(formatted, "`b-c`") {
		t.Fatalf("expected formatted insert keeps quoted identifiers, got %q", formatted)
	}
}

func TestSQLCoverage_DatabaseOptionCombinationAndErrors(t *testing.T) {
	stmt, err := Parse("create database if not exists db_cov comp 2 cachesize 10 maxrows 1000 minrows 100 pages 128 pagesize 4096 tsdb_pagesize 8192 vgroups 8 single_stable 1;")
	if err != nil {
		t.Fatalf("parse create database option combo failed: %v", err)
	}
	cd, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if cd.Options == nil || cd.Options.CompressionLevel != 2 || cd.Options.CacheLastSize != 10 || cd.Options.MaxRowsPerBlock != 1000 || cd.Options.MinRowsPerBlock != 100 || cd.Options.Pages != 128 || cd.Options.Pagesize != 4096 || cd.Options.TsdbPageSize != 8192 || cd.Options.NumOfVgroups != 8 || cd.Options.SingleStable != 1 {
		t.Fatalf("unexpected create database options parsed: %+v", cd.Options)
	}

	if _, err := Parse("create database if not exists db_err comp x;"); err == nil {
		t.Fatalf("expected parse error for invalid comp")
	}
	if _, err := Parse("create database if not exists db_err2 cachesize x;"); err == nil {
		t.Fatalf("expected parse error for invalid cachesize")
	}
	if _, err := Parse("create database if not exists db_err3 vgroups x;"); err == nil {
		t.Fatalf("expected parse error for invalid vgroups")
	}
}
