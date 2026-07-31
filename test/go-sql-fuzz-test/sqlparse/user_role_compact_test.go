package sqlparser

import "testing"

func TestCreateUserStatement_ParseAndBuild(t *testing.T) {
	stmt, err := Parse("create user u1 pass 'p1' enable 0;")
	if err != nil {
		t.Fatalf("parse create user failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.UserName != "u1" {
		t.Fatalf("unexpected user name: %+v", cu)
	}
	if cu.Password != "p1" {
		t.Fatalf("unexpected password: %+v", cu)
	}
	if cu.Enable != 0 {
		t.Fatalf("expected enable=0, got %+v", cu)
	}
}

func TestCreateUserStatement_CreateDbIntegerToken(t *testing.T) {
	stmt, err := Parse("create user u2 pass 'p' createdb 1;")
	if err != nil {
		t.Fatalf("parse create user createdb failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.CreateDb != 1 {
		t.Fatalf("expected createdb=1, got %+v", cu)
	}
}

func TestCreateUserStatement_ChangePassIntegerToken(t *testing.T) {
	stmt, err := Parse("create user u3 pass 'p' changepass 1;")
	if err != nil {
		t.Fatalf("parse create user changepass failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.Changepass != 1 {
		t.Fatalf("expected changepass=1, got %+v", cu)
	}
}

func TestCreateUserStatement_SessionPerUserIntegerToken(t *testing.T) {
	stmt, err := Parse("create user u5 pass 'p' session_per_user 1;")
	if err != nil {
		t.Fatalf("parse create user session_per_user failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.SessionPerUser != 1 {
		t.Fatalf("expected session_per_user=1, got %+v", cu)
	}
}

func TestCreateUserStatement_IsImportIntegerToken(t *testing.T) {
	stmt, err := Parse("create user u6 pass 'p' is_import 1;")
	if err != nil {
		t.Fatalf("parse create user is_import failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.IsImport != 1 {
		t.Fatalf("expected is_import=1, got %+v", cu)
	}
}

func TestAlterUserStatement_Parse(t *testing.T) {
	stmt, err := Parse("alter user u1 enable 1;")
	if err != nil {
		t.Fatalf("parse alter user failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserName != "u1" {
		t.Fatalf("unexpected user name: %+v", au)
	}
	if au.UserOptions == nil || !au.UserOptions.HasEnable {
		t.Fatalf("expected enable option set, got %+v", au.UserOptions)
	}
}

func TestAlterUserStatement_MultiOptionsParse(t *testing.T) {
	stmt, err := Parse("alter user u1 enable 1 totpseed null;")
	if err != nil {
		t.Fatalf("parse alter user multi options failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil || !au.UserOptions.HasEnable || !au.UserOptions.HasTotpseed {
		t.Fatalf("expected merged options, got %+v", au.UserOptions)
	}
	if au.UserOptions.Enable != 1 || au.UserOptions.Totpseed != "" {
		t.Fatalf("unexpected merged option values: %+v", au.UserOptions)
	}
}

func TestAlterUserStatement_SysinfoIntegerParse(t *testing.T) {
	stmt, err := Parse("alter user u1 sysinfo 0;")
	if err != nil {
		t.Fatalf("parse alter user sysinfo failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil || !au.UserOptions.HasSysinfo || au.UserOptions.Sysinfo != 0 {
		t.Fatalf("unexpected sysinfo options: %+v", au.UserOptions)
	}
}

func TestAlterUserStatement_PassStringParse(t *testing.T) {
	stmt, err := Parse("alter user u1 pass 'p2';")
	if err != nil {
		t.Fatalf("parse alter user pass failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil || !au.UserOptions.HasPassword || au.UserOptions.Password != "p2" {
		t.Fatalf("unexpected password options: %+v", au.UserOptions)
	}
}

func TestAlterUserStatement_HostListStringToken(t *testing.T) {
	stmt1, err := Parse("alter user u1 add host '127.0.0.1/32';")
	if err != nil {
		t.Fatalf("parse alter user add host failed: %v", err)
	}
	au1, ok := stmt1.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt1)
	}
	if au1.UserOptions == nil || len(au1.UserOptions.IpRanges) == 0 {
		t.Fatalf("expected add host list, got %+v", au1.UserOptions)
	}

	stmt2, err := Parse("alter user u1 drop host '127.0.0.1/32';")
	if err != nil {
		t.Fatalf("parse alter user drop host failed: %v", err)
	}
	au2, ok := stmt2.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt2)
	}
	if au2.UserOptions == nil || len(au2.UserOptions.DropIpRanges) == 0 {
		t.Fatalf("expected drop host list, got %+v", au2.UserOptions)
	}
}

func TestAlterUserStatement_DateTimeListStringToken(t *testing.T) {
	stmt1, err := Parse("alter user u1 add allow_datetime '1d';")
	if err != nil {
		t.Fatalf("parse alter user add allow_datetime failed: %v", err)
	}
	au1, ok := stmt1.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt1)
	}
	if au1.UserOptions == nil || len(au1.UserOptions.TimeRanges) == 0 || au1.UserOptions.TimeRanges[0] == nil {
		t.Fatalf("expected non-empty datetime ranges, got %+v", au1.UserOptions)
	}

	stmt2, err := Parse("alter user u1 drop allow_datetime '1d';")
	if err != nil {
		t.Fatalf("parse alter user drop allow_datetime failed: %v", err)
	}
	au2, ok := stmt2.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt2)
	}
	if au2.UserOptions == nil || len(au2.UserOptions.DropTimeRanges) == 0 || au2.UserOptions.DropTimeRanges[0] == nil {
		t.Fatalf("expected non-empty drop datetime ranges, got %+v", au2.UserOptions)
	}
}

func TestDropUserStatement_Parse(t *testing.T) {
	stmt, err := Parse("drop user if exists u1;")
	if err != nil {
		t.Fatalf("parse drop user failed: %v", err)
	}
	du, ok := stmt.(*DropUserStmt)
	if !ok {
		t.Fatalf("expected *DropUserStmt, got %T", stmt)
	}
	if du.UserName != "u1" || !du.IgnoreNotExist {
		t.Fatalf("unexpected drop user stmt: %+v", du)
	}
}

func TestRoleStatements_ParseAndBuild(t *testing.T) {
	stmt, err := Parse("create role if not exists r1;")
	if err != nil {
		t.Fatalf("parse create role failed: %v", err)
	}
	cr, ok := stmt.(*CreateRoleStmt)
	if !ok {
		t.Fatalf("expected *CreateRoleStmt, got %T", stmt)
	}
	if cr.Name != "r1" || !cr.IgnoreExists {
		t.Fatalf("unexpected create role stmt: %+v", cr)
	}

	stmt, err = Parse("drop role if exists r1;")
	if err != nil {
		t.Fatalf("parse drop role failed: %v", err)
	}
	dr, ok := stmt.(*DropRoleStmt)
	if !ok {
		t.Fatalf("expected *DropRoleStmt, got %T", stmt)
	}
	if dr.Name != "r1" || !dr.IgnoreExists {
		t.Fatalf("unexpected drop role stmt: %+v", dr)
	}

	stmt, err = Parse("lock role r1;")
	if err != nil {
		t.Fatalf("parse lock role failed: %v", err)
	}
	ar, ok := stmt.(*AlterRoleStmt)
	if !ok {
		t.Fatalf("expected *AlterRoleStmt, got %T", stmt)
	}
	if ar.Name != "r1" || ar.Action != TSDB_ALTER_ROLE_LOCK || ar.Value.Type != 1 {
		t.Fatalf("unexpected lock role stmt: %+v", ar)
	}

	stmt, err = Parse("unlock role r1;")
	if err != nil {
		t.Fatalf("parse unlock role failed: %v", err)
	}
	ar2, ok := stmt.(*AlterRoleStmt)
	if !ok {
		t.Fatalf("expected *AlterRoleStmt, got %T", stmt)
	}
	if ar2.Name != "r1" || ar2.Action != TSDB_ALTER_ROLE_LOCK || ar2.Value.Type != 0 {
		t.Fatalf("unexpected unlock role stmt: %+v", ar2)
	}

	stmt, err = Parse("grant role r1 to u1;")
	if err != nil {
		t.Fatalf("parse grant role failed: %v", err)
	}
	gr, ok := stmt.(*GrantRoleStmt)
	if !ok {
		t.Fatalf("expected *GrantRoleStmt, got %T", stmt)
	}
	if gr.RoleName != "r1" || gr.GranteeName != "u1" || gr.Action != TSDB_ALTER_ROLE_ROLE {
		t.Fatalf("unexpected grant role stmt: %+v", gr)
	}

	stmt, err = Parse("revoke role r1 from u1;")
	if err != nil {
		t.Fatalf("parse revoke role failed: %v", err)
	}
	rr, ok := stmt.(*RevokeRoleStmt)
	if !ok {
		t.Fatalf("expected *RevokeRoleStmt, got %T", stmt)
	}
	if rr.RoleName != "r1" || rr.RevokeeName != "u1" || rr.Action != TSDB_ALTER_ROLE_ROLE {
		t.Fatalf("unexpected revoke role stmt: %+v", rr)
	}
}

func TestMergeUserOptions_CombineAndOverride(t *testing.T) {
	a := CreateDefaultUserOptions()
	a.HasEnable = true
	a.Enable = 1

	b := &UserOptions{
		HasEnable:  true,
		Enable:     0,
		HasSysinfo: true,
		Sysinfo:    0,
	}

	m := MergeUserOptions(nil, a, b)
	if m == nil {
		t.Fatalf("expected merged options")
	}
	if !m.HasEnable || m.Enable != 0 {
		t.Fatalf("expected enable overridden to 0, got %+v", m)
	}
	if !m.HasSysinfo || m.Sysinfo != 0 {
		t.Fatalf("expected sysinfo merged, got %+v", m)
	}
}

func TestMergeUserOptions_DefaultAndClone(t *testing.T) {
	m0 := MergeUserOptions(nil, nil, nil)
	if m0 == nil {
		t.Fatalf("expected default user options")
	}

	src := &UserOptions{HasEnable: true, Enable: 1}
	m1 := MergeUserOptions(nil, src, nil)
	if m1 == nil || !m1.HasEnable || m1.Enable != 1 {
		t.Fatalf("unexpected clone result: %+v", m1)
	}
	m2 := MergeUserOptions(nil, nil, src)
	if m2 == nil || !m2.HasEnable || m2.Enable != 1 {
		t.Fatalf("unexpected clone result: %+v", m2)
	}
}

func TestUserOption_TimeUnitSemantics_AlignedWithLemon(t *testing.T) {
	stmt, err := Parse("create user u_time pass 'p' connect_time 2 connect_idle_time 3 password_life_time 4 password_reuse_time 5 password_lock_time 6 password_grace_time 7 inactive_account_time 8;")
	if err != nil {
		t.Fatalf("parse create user with time options failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if cu.ConnectTime != 120 {
		t.Fatalf("connect_time expected 120 seconds, got %d", cu.ConnectTime)
	}
	if cu.ConnectIdleTime != 180 {
		t.Fatalf("connect_idle_time expected 180 seconds, got %d", cu.ConnectIdleTime)
	}
	if cu.PasswordLifeTime != 4*1440*60 {
		t.Fatalf("password_life_time expected day->seconds, got %d", cu.PasswordLifeTime)
	}
	if cu.PasswordReuseTime != 5*1440*60 {
		t.Fatalf("password_reuse_time expected day->seconds, got %d", cu.PasswordReuseTime)
	}
	if cu.PasswordLockTime != 360 {
		t.Fatalf("password_lock_time expected minute->seconds, got %d", cu.PasswordLockTime)
	}
	if cu.PasswordGraceTime != 7*1440*60 {
		t.Fatalf("password_grace_time expected day->seconds, got %d", cu.PasswordGraceTime)
	}
	if cu.InactiveAccountTime != 8*1440*60 {
		t.Fatalf("inactive_account_time expected day->seconds, got %d", cu.InactiveAccountTime)
	}
}
