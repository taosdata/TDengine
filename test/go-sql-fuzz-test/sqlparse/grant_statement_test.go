package sqlparser

import "testing"

func TestGrantPrivilegeStatements_All(t *testing.T) {
	stmt, err := Parse("grant all to u1;")
	if err != nil {
		t.Fatalf("parse grant all failed: %v", err)
	}
	g, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g.OptrType != 0 || g.Principal != "u1" || g.Privileges.PrivArgs != PRIV_CM_ALL || g.PrivilegeName != "all" {
		t.Fatalf("unexpected grant stmt: %+v", g)
	}

	stmt2, err := Parse("grant all privileges to u1;")
	if err != nil {
		t.Fatalf("parse grant all privileges failed: %v", err)
	}
	g2, ok := stmt2.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt2)
	}
	if g2.OptrType != 0 || g2.Principal != "u1" || g2.Privileges.PrivArgs != PRIV_CM_ALL || g2.PrivilegeName != "all" {
		t.Fatalf("unexpected grant privileges stmt: %+v", g2)
	}
}

func TestRevokePrivilegeStatements_All(t *testing.T) {
	stmt, err := Parse("revoke all from u1;")
	if err != nil {
		t.Fatalf("parse revoke all failed: %v", err)
	}
	g, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g.OptrType != 1 || g.Principal != "u1" || g.Privileges.PrivArgs != PRIV_CM_ALL || g.PrivilegeName != "all" {
		t.Fatalf("unexpected revoke stmt: %+v", g)
	}

	stmt2, err := Parse("revoke all privileges from u1;")
	if err != nil {
		t.Fatalf("parse revoke all privileges failed: %v", err)
	}
	g2, ok := stmt2.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt2)
	}
	if g2.OptrType != 1 || g2.Principal != "u1" || g2.Privileges.PrivArgs != PRIV_CM_ALL || g2.PrivilegeName != "all" {
		t.Fatalf("unexpected revoke privileges stmt: %+v", g2)
	}
}

func TestGrantRevokePrivilegeStatements_ReadWrite(t *testing.T) {
	stmt, err := Parse("grant read to u1;")
	if err != nil {
		t.Fatalf("parse grant read failed: %v", err)
	}
	gr, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gr.OptrType != 0 || gr.Principal != "u1" || gr.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || gr.PrivilegeName != "read" {
		t.Fatalf("unexpected grant read stmt: %+v", gr)
	}

	stmt, err = Parse("grant write to u1;")
	if err != nil {
		t.Fatalf("parse grant write failed: %v", err)
	}
	gw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gw.OptrType != 0 || gw.Principal != "u1" || gw.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || gw.PrivilegeName != "write" {
		t.Fatalf("unexpected grant write stmt: %+v", gw)
	}

	stmt, err = Parse("revoke read from u1;")
	if err != nil {
		t.Fatalf("parse revoke read failed: %v", err)
	}
	rr, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rr.OptrType != 1 || rr.Principal != "u1" || rr.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || rr.PrivilegeName != "read" {
		t.Fatalf("unexpected revoke read stmt: %+v", rr)
	}

	stmt, err = Parse("revoke write from u1;")
	if err != nil {
		t.Fatalf("parse revoke write failed: %v", err)
	}
	rw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rw.OptrType != 1 || rw.Principal != "u1" || rw.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || rw.PrivilegeName != "write" {
		t.Fatalf("unexpected revoke write stmt: %+v", rw)
	}
}

func TestGrantRevokePrivilegeStatements_Alter(t *testing.T) {
	stmt, err := Parse("grant alter to u1;")
	if err != nil {
		t.Fatalf("parse grant alter failed: %v", err)
	}
	ga, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if ga.OptrType != 0 || ga.Principal != "u1" || ga.Privileges.PrivArgs != PRIV_CM_ALTER || ga.PrivilegeName != "alter" {
		t.Fatalf("unexpected grant alter stmt: %+v", ga)
	}

	stmt, err = Parse("revoke alter from u1;")
	if err != nil {
		t.Fatalf("parse revoke alter failed: %v", err)
	}
	ra, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if ra.OptrType != 1 || ra.Principal != "u1" || ra.Privileges.PrivArgs != PRIV_CM_ALTER || ra.PrivilegeName != "alter" {
		t.Fatalf("unexpected revoke alter stmt: %+v", ra)
	}
}

func TestGrantRevokePrivilegeStatements_ShowCreate(t *testing.T) {
	stmt, err := Parse("grant show create to u1;")
	if err != nil {
		t.Fatalf("parse grant show create failed: %v", err)
	}
	g, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g.OptrType != 0 || g.Principal != "u1" || g.Privileges.PrivArgs != PRIV_CM_SHOW_CREATE || g.PrivilegeName != "show create" {
		t.Fatalf("unexpected grant show create stmt: %+v", g)
	}
	if got := formatStatementForRoundTrip(t, g); got != "grant show create to u1" {
		t.Fatalf("unexpected format for grant show create: %q", got)
	}

	stmt, err = Parse("revoke show create from u1;")
	if err != nil {
		t.Fatalf("parse revoke show create failed: %v", err)
	}
	r, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if r.OptrType != 1 || r.Principal != "u1" || r.Privileges.PrivArgs != PRIV_CM_SHOW_CREATE || r.PrivilegeName != "show create" {
		t.Fatalf("unexpected revoke show create stmt: %+v", r)
	}
	if got := formatStatementForRoundTrip(t, r); got != "revoke show create from u1" {
		t.Fatalf("unexpected format for revoke show create: %q", got)
	}
}

func TestGrantRevokePrivilegeStatements_Extended(t *testing.T) {
	stmt, err := Parse("grant create database to u1;")
	if err != nil {
		t.Fatalf("parse grant create database failed: %v", err)
	}
	gcd, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gcd.OptrType != 0 || gcd.Principal != "u1" || gcd.Privileges.PrivArgs != PRIV_DB_CREATE || gcd.PrivilegeName != "create database" {
		t.Fatalf("unexpected grant create database stmt: %+v", gcd)
	}
	if got := formatStatementForRoundTrip(t, gcd); got != "grant create database to u1" {
		t.Fatalf("unexpected format for grant create database: %q", got)
	}

	stmt, err = Parse("revoke show users from u1;")
	if err != nil {
		t.Fatalf("parse revoke show users failed: %v", err)
	}
	rsu, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rsu.OptrType != 1 || rsu.Principal != "u1" || rsu.Privileges.PrivArgs != PRIV_USER_SHOW || rsu.PrivilegeName != "show users" {
		t.Fatalf("unexpected revoke show users stmt: %+v", rsu)
	}
	if got := formatStatementForRoundTrip(t, rsu); got != "revoke show users from u1" {
		t.Fatalf("unexpected format for revoke show users: %q", got)
	}
}

func TestGrantPrivilegeStatements_PrivArgBranches(t *testing.T) {
	cases := []struct {
		sql      string
		privArg  int32
		privName string
	}{
		{"grant alter to u1;", PRIV_CM_ALTER, "alter"},
		{"grant drop to u1;", PRIV_CM_DROP, "drop"},
		{"grant show to u1;", PRIV_CM_SHOW, "show"},
		{"grant show create to u1;", PRIV_CM_SHOW_CREATE, "show create"},
		{"grant start to u1;", PRIV_CM_START, "start"},
		{"grant stop to u1;", PRIV_CM_STOP, "stop"},
		{"grant kill to u1;", PRIV_CM_KILL, "kill"},
		{"grant recalculate to u1;", PRIV_CM_RECALC, "recalculate"},
		{"grant subscribe to u1;", PRIV_CM_SUBSCRIBE, "subscribe"},
		{"grant create database to u1;", PRIV_DB_CREATE, "create database"},
		{"grant use database to u1;", PRIV_DB_USE, "use database"},
		{"grant flush database to u1;", PRIV_DB_FLUSH, "flush database"},
		{"grant compact database to u1;", PRIV_DB_COMPACT, "compact database"},
		{"grant trim database to u1;", PRIV_DB_TRIM, "trim database"},
		{"grant rollup database to u1;", PRIV_DB_ROLLUP, "rollup database"},
		{"grant scan database to u1;", PRIV_DB_SCAN, "scan database"},
		{"grant ssmigrate database to u1;", PRIV_DB_SSMIGRATE, "ssmigrate database"},
		{"grant create table to u1;", PRIV_TBL_CREATE, "create table"},
		{"grant create user to u1;", PRIV_USER_CREATE, "create user"},
		{"grant drop user to u1;", PRIV_USER_DROP, "drop user"},
		{"grant show users to u1;", PRIV_USER_SHOW, "show users"},
		{"grant create role to u1;", PRIV_ROLE_CREATE, "create role"},
		{"grant drop role to u1;", PRIV_ROLE_DROP, "drop role"},
		{"grant show roles to u1;", PRIV_ROLE_SHOW, "show roles"},
		{"grant create stream to u1;", PRIV_STREAM_CREATE, "create stream"},
		{"grant create topic to u1;", PRIV_TOPIC_CREATE, "create topic"},
		{"grant show consumers to u1;", PRIV_CONSUMER_SHOW, "show consumers"},
		{"grant show subscriptions to u1;", PRIV_SUBSCRIPTION_SHOW, "show subscriptions"},
		{"grant show trans to u1;", PRIV_TRANS_SHOW, "show trans"},
		{"grant kill trans to u1;", PRIV_TRANS_KILL, "kill trans"},
		{"grant show connections to u1;", PRIV_CONNECTION_SHOW, "show connections"},
		{"grant kill connection to u1;", PRIV_CONNECTION_KILL, "kill connection"},
		{"grant show queries to u1;", PRIV_QUERY_SHOW, "show queries"},
		{"grant kill query to u1;", PRIV_QUERY_KILL, "kill query"},
		{"grant show grants to u1;", PRIV_GRANTS_SHOW, "show grants"},
		{"grant show cluster to u1;", PRIV_CLUSTER_SHOW, "show cluster"},
		{"grant show apps to u1;", PRIV_APPS_SHOW, "show apps"},
	}

	for _, tc := range cases {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tc.sql, err)
		}
		g, ok := stmt.(*GrantStmt)
		if !ok {
			t.Fatalf("expected *GrantStmt, got %T for %q", stmt, tc.sql)
		}
		if g.OptrType != 0 || g.Principal != "u1" || g.Privileges.PrivArgs != tc.privArg || g.PrivilegeName != tc.privName {
			t.Fatalf("unexpected grant stmt for %q: %+v", tc.sql, g)
		}
	}
}

func TestGrantPrivNameFromArg_Coverage(t *testing.T) {
	if got := grantPrivNameFromArg(PRIV_CM_ALL); got != "all" {
		t.Fatalf("unexpected PRIV_CM_ALL name: %q", got)
	}
	if got := grantPrivNameFromArg(PRIV_TYPE_UNKNOWN); got != "read" {
		t.Fatalf("unexpected PRIV_TYPE_UNKNOWN name: %q", got)
	}
	if got := grantPrivNameFromArg(99999); got != "read" {
		t.Fatalf("unexpected default name: %q", got)
	}
}

func TestGrantPrivArgFromName_Coverage(t *testing.T) {
	cases := map[string]int32{
		"all":                PRIV_CM_ALL,
		"alter":              PRIV_CM_ALTER,
		"drop":               PRIV_CM_DROP,
		"show":               PRIV_CM_SHOW,
		"show create":        PRIV_CM_SHOW_CREATE,
		"start":              PRIV_CM_START,
		"stop":               PRIV_CM_STOP,
		"kill":               PRIV_CM_KILL,
		"recalculate":        PRIV_CM_RECALC,
		"subscribe":          PRIV_CM_SUBSCRIBE,
		"create database":    PRIV_DB_CREATE,
		"use database":       PRIV_DB_USE,
		"flush database":     PRIV_DB_FLUSH,
		"compact database":   PRIV_DB_COMPACT,
		"trim database":      PRIV_DB_TRIM,
		"rollup database":    PRIV_DB_ROLLUP,
		"scan database":      PRIV_DB_SCAN,
		"ssmigrate database": PRIV_DB_SSMIGRATE,
		"create table":       PRIV_TBL_CREATE,
		"create user":        PRIV_USER_CREATE,
		"drop user":          PRIV_USER_DROP,
		"show users":         PRIV_USER_SHOW,
		"create role":        PRIV_ROLE_CREATE,
		"drop role":          PRIV_ROLE_DROP,
		"show roles":         PRIV_ROLE_SHOW,
		"create stream":      PRIV_STREAM_CREATE,
		"create topic":       PRIV_TOPIC_CREATE,
		"show consumers":     PRIV_CONSUMER_SHOW,
		"show subscriptions": PRIV_SUBSCRIPTION_SHOW,
		"show trans":         PRIV_TRANS_SHOW,
		"kill trans":         PRIV_TRANS_KILL,
		"show connections":   PRIV_CONNECTION_SHOW,
		"kill connection":    PRIV_CONNECTION_KILL,
		"show queries":       PRIV_QUERY_SHOW,
		"kill query":         PRIV_QUERY_KILL,
		"show grants":        PRIV_GRANTS_SHOW,
		"show cluster":       PRIV_CLUSTER_SHOW,
		"show apps":          PRIV_APPS_SHOW,
		"read":               PRIV_TYPE_UNKNOWN,
		"write":              PRIV_TYPE_UNKNOWN,
		"unknown_x":          PRIV_TYPE_UNKNOWN,
	}
	for name, want := range cases {
		if got := grantPrivArgFromName(name); got != want {
			t.Fatalf("grantPrivArgFromName(%q)=%d want=%d", name, got, want)
		}
	}
}

func TestApplyGrantLevel_Coverage(t *testing.T) {
	applyGrantLevel("db1", nil)

	s := &GrantStmt{}
	applyGrantLevel("db1", s)
	if s.Privileges.ObjType != PRIV_OBJ_DB || s.ObjName != "db1" {
		t.Fatalf("unexpected fallback grant level mapping: %+v", s)
	}
}

func TestGrantRevokePrivilegeStatements_OnAndWith(t *testing.T) {
	stmt, err := Parse("grant create table on database db1 to u1;")
	if err != nil {
		t.Fatalf("parse grant on database failed: %v", err)
	}
	gdb, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gdb.OptrType != 0 || gdb.Principal != "u1" || gdb.Privileges.ObjType != PRIV_OBJ_DB || gdb.ObjName != "db1" || gdb.PrivilegeName != "create table" {
		t.Fatalf("unexpected grant on database stmt: %+v", gdb)
	}
	if got := formatStatementForRoundTrip(t, gdb); got != "grant create table on database db1 to u1" {
		t.Fatalf("unexpected format for grant on database: %q", got)
	}

	stmt, err = Parse("revoke drop on table db1.t1 from u1;")
	if err != nil {
		t.Fatalf("parse revoke on table failed: %v", err)
	}
	rtb, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rtb.OptrType != 1 || rtb.Principal != "u1" || rtb.Privileges.ObjType != PRIV_OBJ_TBL || rtb.ObjName != "db1" || rtb.TabName != "t1" || rtb.PrivilegeName != "drop" {
		t.Fatalf("unexpected revoke on table stmt: %+v", rtb)
	}
	if got := formatStatementForRoundTrip(t, rtb); got != "revoke drop on table db1.t1 from u1" {
		t.Fatalf("unexpected format for revoke on table: %q", got)
	}

	stmt, err = Parse("grant show with v > 1 to u1;")
	if err != nil {
		t.Fatalf("parse grant with condition failed: %v", err)
	}
	gw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gw.OptrType != 0 || gw.Principal != "u1" || gw.PrivilegeName != "show" || gw.Cond == nil {
		t.Fatalf("unexpected grant with stmt: %+v", gw)
	}
	if got := formatStatementForRoundTrip(t, gw); got != "grant show with v > 1 to u1" {
		t.Fatalf("unexpected format for grant with: %q", got)
	}

	stmt, err = Parse("revoke kill with v > 1 from u1;")
	if err != nil {
		t.Fatalf("parse revoke with condition failed: %v", err)
	}
	rw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rw.OptrType != 1 || rw.Principal != "u1" || rw.PrivilegeName != "kill" || rw.Cond == nil {
		t.Fatalf("unexpected revoke with stmt: %+v", rw)
	}
	if got := formatStatementForRoundTrip(t, rw); got != "revoke kill with v > 1 from u1" {
		t.Fatalf("unexpected format for revoke with: %q", got)
	}

	stmt, err = Parse("grant create table on database db1 with v > 1 to u1;")
	if err != nil {
		t.Fatalf("parse grant on database with failed: %v", err)
	}
	gdbw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gdbw.OptrType != 0 || gdbw.Privileges.ObjType != PRIV_OBJ_DB || gdbw.ObjName != "db1" || gdbw.Cond == nil {
		t.Fatalf("unexpected grant on database with stmt: %+v", gdbw)
	}
	if got := formatStatementForRoundTrip(t, gdbw); got != "grant create table on database db1 with v > 1 to u1" {
		t.Fatalf("unexpected format for grant on database with: %q", got)
	}

	stmt, err = Parse("revoke drop on table db1.t1 with v > 1 from u1;")
	if err != nil {
		t.Fatalf("parse revoke on table with failed: %v", err)
	}
	rtbw, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rtbw.OptrType != 1 || rtbw.Privileges.ObjType != PRIV_OBJ_TBL || rtbw.ObjName != "db1" || rtbw.TabName != "t1" || rtbw.Cond == nil {
		t.Fatalf("unexpected revoke on table with stmt: %+v", rtbw)
	}
	if got := formatStatementForRoundTrip(t, rtbw); got != "revoke drop on table db1.t1 with v > 1 from u1" {
		t.Fatalf("unexpected format for revoke on table with: %q", got)
	}
}

func TestGrantRevokePrivilegeStatements_List(t *testing.T) {
	stmt, err := Parse("grant read, write, alter to u1;")
	if err != nil {
		t.Fatalf("parse grant list failed: %v", err)
	}
	gl, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if gl.OptrType != 0 || gl.Principal != "u1" || gl.PrivilegeName != "read, write, alter" || gl.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN {
		t.Fatalf("unexpected grant list stmt: %+v", gl)
	}
	if got := formatStatementForRoundTrip(t, gl); got != "grant read, write, alter to u1" {
		t.Fatalf("unexpected format for grant list: %q", got)
	}

	stmt, err = Parse("revoke all privileges, show users from u1;")
	if err != nil {
		t.Fatalf("parse revoke list failed: %v", err)
	}
	rl, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if rl.OptrType != 1 || rl.Principal != "u1" || rl.PrivilegeName != "all, show users" || rl.Privileges.PrivArgs != PRIV_CM_ALL {
		t.Fatalf("unexpected revoke list stmt: %+v", rl)
	}
	if got := formatStatementForRoundTrip(t, rl); got != "revoke all, show users from u1" {
		t.Fatalf("unexpected format for revoke list: %q", got)
	}
}

func TestGrantRevokePrivilegeStatements_WildcardLevels(t *testing.T) {
	stmt, err := Parse("grant show on * to u1;")
	if err != nil {
		t.Fatalf("parse grant on * failed: %v", err)
	}
	g1, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g1.ObjName != "*" || g1.TabName != "" || g1.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected grant on * stmt: %+v", g1)
	}
	if got := formatStatementForRoundTrip(t, g1); got != "grant show on table * to u1" {
		t.Fatalf("unexpected format for grant on *: %q", got)
	}

	stmt, err = Parse("grant show on *.* to u1;")
	if err != nil {
		t.Fatalf("parse grant on *.* failed: %v", err)
	}
	g2, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g2.ObjName != "*" || g2.TabName != "*" || g2.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected grant on *.* stmt: %+v", g2)
	}
	if got := formatStatementForRoundTrip(t, g2); got != "grant show on table *.* to u1" {
		t.Fatalf("unexpected format for grant on *.*: %q", got)
	}

	stmt, err = Parse("revoke drop on db1.* from u1;")
	if err != nil {
		t.Fatalf("parse revoke on db1.* failed: %v", err)
	}
	r3, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if r3.ObjName != "db1" || r3.TabName != "*" || r3.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected revoke on db1.* stmt: %+v", r3)
	}
	if got := formatStatementForRoundTrip(t, r3); got != "revoke drop on table db1.* from u1" {
		t.Fatalf("unexpected format for revoke on db1.*: %q", got)
	}

	stmt, err = Parse("grant show on table db1.* to u1;")
	if err != nil {
		t.Fatalf("parse grant on table db1.* failed: %v", err)
	}
	g4, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if g4.ObjName != "db1" || g4.TabName != "*" || g4.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected grant on table db1.* stmt: %+v", g4)
	}

	stmt, err = Parse("revoke show on table * from u1;")
	if err != nil {
		t.Fatalf("parse revoke on table * failed: %v", err)
	}
	r4, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if r4.ObjName != "*" || r4.TabName != "" || r4.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected revoke on table * stmt: %+v", r4)
	}

	stmt, err = Parse("revoke show on table *.* from u1;")
	if err != nil {
		t.Fatalf("parse revoke on table *.* failed: %v", err)
	}
	r5, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}
	if r5.ObjName != "*" || r5.TabName != "*" || r5.Privileges.ObjType != PRIV_OBJ_TBL {
		t.Fatalf("unexpected revoke on table *.* stmt: %+v", r5)
	}
}

func TestGrantStmt_Format_TableTabNameOnly(t *testing.T) {
	stmt := &GrantStmt{
		OptrType:      0,
		Principal:     "u1",
		PrivilegeName: "read",
		Privileges: PrivSetArgs{
			ObjType: PRIV_OBJ_TBL,
		},
		TabName: "t_only",
	}
	if got := formatStatementForRoundTrip(t, stmt); got != "grant read on table t_only to u1" {
		t.Fatalf("unexpected grant table-only format: %q", got)
	}
}
