package sqlparser

import "testing"

func TestUserSQLCoverage_CreateUserOptionValueModes(t *testing.T) {
	cases := []struct {
		name  string
		sql   string
		check func(t *testing.T, s *CreateUserStmt)
	}{
		{
			name: "session_per_user_default",
			sql:  "create user u1 pass 'p' session_per_user default;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.SessionPerUser != TSDB_USER_SESSION_PER_USER_DEFAULT {
					t.Fatalf("unexpected session_per_user default: %d", s.SessionPerUser)
				}
			},
		},
		{
			name: "session_per_user_unlimited",
			sql:  "create user u1 pass 'p' session_per_user unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.SessionPerUser != -1 {
					t.Fatalf("unexpected session_per_user unlimited: %d", s.SessionPerUser)
				}
			},
		},
		{
			name: "connect_time_default",
			sql:  "create user u1 pass 'p' connect_time default;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.ConnectTime != TSDB_USER_CONNECT_TIME_DEFAULT {
					t.Fatalf("unexpected connect_time default: %d", s.ConnectTime)
				}
			},
		},
		{
			name: "connect_time_unlimited",
			sql:  "create user u1 pass 'p' connect_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.ConnectTime != -1 {
					t.Fatalf("unexpected connect_time unlimited: %d", s.ConnectTime)
				}
			},
		},
		{
			name: "connect_idle_time_unlimited",
			sql:  "create user u1 pass 'p' connect_idle_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.ConnectIdleTime != -1 {
					t.Fatalf("unexpected connect_idle_time unlimited: %d", s.ConnectIdleTime)
				}
			},
		},
		{
			name: "call_per_session_unlimited",
			sql:  "create user u1 pass 'p' call_per_session unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.CallPerSession != -1 {
					t.Fatalf("unexpected call_per_session unlimited: %d", s.CallPerSession)
				}
			},
		},
		{
			name: "vnode_per_call_unlimited",
			sql:  "create user u1 pass 'p' vnode_per_call unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.VnodePerCall != -1 {
					t.Fatalf("unexpected vnode_per_call unlimited: %d", s.VnodePerCall)
				}
			},
		},
		{
			name: "failed_login_attempts_unlimited",
			sql:  "create user u1 pass 'p' failed_login_attempts unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.FailedLoginAttempts != -1 {
					t.Fatalf("unexpected failed_login_attempts unlimited: %d", s.FailedLoginAttempts)
				}
			},
		},
		{
			name: "password_life_time_unlimited",
			sql:  "create user u1 pass 'p' password_life_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.PasswordLifeTime != -1 {
					t.Fatalf("unexpected password_life_time unlimited: %d", s.PasswordLifeTime)
				}
			},
		},
		{
			name: "password_lock_time_unlimited",
			sql:  "create user u1 pass 'p' password_lock_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.PasswordLockTime != -1 {
					t.Fatalf("unexpected password_lock_time unlimited: %d", s.PasswordLockTime)
				}
			},
		},
		{
			name: "password_grace_time_unlimited",
			sql:  "create user u1 pass 'p' password_grace_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.PasswordGraceTime != -1 {
					t.Fatalf("unexpected password_grace_time unlimited: %d", s.PasswordGraceTime)
				}
			},
		},
		{
			name: "inactive_account_time_unlimited",
			sql:  "create user u1 pass 'p' inactive_account_time unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.InactiveAccountTime != -1 {
					t.Fatalf("unexpected inactive_account_time unlimited: %d", s.InactiveAccountTime)
				}
			},
		},
		{
			name: "allow_token_num_unlimited",
			sql:  "create user u1 pass 'p' allow_token_num unlimited;",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.AllowTokenNum != -1 {
					t.Fatalf("unexpected allow_token_num unlimited: %d", s.AllowTokenNum)
				}
			},
		},
		{
			name: "totpseed_string",
			sql:  "create user u1 pass 'p' totpseed 'abc';",
			check: func(t *testing.T, s *CreateUserStmt) {
				if s.Totpseed != "abc" {
					t.Fatalf("unexpected totpseed: %q", s.Totpseed)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse failed for %q: %v", tc.sql, err)
			}
			cu, ok := stmt.(*CreateUserStmt)
			if !ok {
				t.Fatalf("expected *CreateUserStmt, got %T", stmt)
			}
			tc.check(t, cu)
		})
	}
}

func TestUserSQLCoverage_CreateUserAllowTokenNumUnlimited_RoundTrip(t *testing.T) {
	sql := "create user ux pass 'p' allow_token_num unlimited;"
	stmt1, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	s1 := formatStatementForRoundTrip(t, stmt1)
	if got := s1; got != "create user ux pass 'p' allow_token_num unlimited" {
		t.Fatalf("unexpected formatted sql: %q", got)
	}
	stmt2, err := Parse(s1)
	if err != nil {
		t.Fatalf("parse formatted failed: %v", err)
	}
	if !statementsSemanticallyEqual(stmt1, stmt2) {
		t.Fatalf("statement mismatch after roundtrip\nstmt1=%#v\nstmt2=%#v", stmt1, stmt2)
	}
}

func TestUserSQLCoverage_CreateUserHostAndDateTime_RoundTrip(t *testing.T) {
	sql := "create user uhost pass 'p' host '127.0.0.1/32' allow_datetime '2024-01-02 03:04:05';"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	formatted := formatStatementForRoundTrip(t, stmt)
	if formatted != "create user uhost pass 'p' host '127.0.0.1/32' allow_datetime '2024-01-02 03:04'" {
		t.Fatalf("unexpected formatted sql: %q", formatted)
	}
	runStatementRoundTrip(t, sql)
}

func TestUserSQLCoverage_CreateUserNotAllowHostAndDateTime_RoundTrip(t *testing.T) {
	sql := "create user uhost2 pass 'p' not_allow_host '10.0.0.0/24' not_allow_datetime '2024-01-02 03:04';"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	formatted := formatStatementForRoundTrip(t, stmt)
	if formatted != "create user uhost2 pass 'p' not_allow_host '10.0.0.0/24' not_allow_datetime '2024-01-02 03:04'" {
		t.Fatalf("unexpected formatted sql: %q", formatted)
	}
	runStatementRoundTrip(t, sql)
}

func TestUserSQLCoverage_AlterUserNotAllowVariants(t *testing.T) {
	cases := []struct {
		name  string
		sql   string
		check func(t *testing.T, s *AlterUserStmt)
	}{
		{
			name: "add_not_allow_host",
			sql:  "alter user u1 add not_allow_host '127.0.0.1/32';",
			check: func(t *testing.T, s *AlterUserStmt) {
				if len(s.UserOptions.IpRanges) != 1 || s.UserOptions.IpRanges[0].Neg != 1 {
					t.Fatalf("unexpected add not_allow_host: %+v", s.UserOptions)
				}
			},
		},
		{
			name: "drop_not_allow_host",
			sql:  "alter user u1 drop not_allow_host '127.0.0.1/32';",
			check: func(t *testing.T, s *AlterUserStmt) {
				if len(s.UserOptions.DropIpRanges) != 1 || s.UserOptions.DropIpRanges[0].Neg != 1 {
					t.Fatalf("unexpected drop not_allow_host: %+v", s.UserOptions)
				}
			},
		},
		{
			name: "add_not_allow_datetime",
			sql:  "alter user u1 add not_allow_datetime '1d';",
			check: func(t *testing.T, s *AlterUserStmt) {
				if len(s.UserOptions.TimeRanges) != 1 || s.UserOptions.TimeRanges[0].Neg != 1 {
					t.Fatalf("unexpected add not_allow_datetime: %+v", s.UserOptions)
				}
			},
		},
		{
			name: "drop_not_allow_datetime",
			sql:  "alter user u1 drop not_allow_datetime '1d';",
			check: func(t *testing.T, s *AlterUserStmt) {
				if len(s.UserOptions.DropTimeRanges) != 1 || s.UserOptions.DropTimeRanges[0].Neg != 1 {
					t.Fatalf("unexpected drop not_allow_datetime: %+v", s.UserOptions)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse failed for %q: %v", tc.sql, err)
			}
			au, ok := stmt.(*AlterUserStmt)
			if !ok {
				t.Fatalf("expected *AlterUserStmt, got %T", stmt)
			}
			if au.UserOptions == nil {
				t.Fatalf("expected UserOptions not nil")
			}
			tc.check(t, au)
		})
	}
}

func TestUserSQLCoverage_AlterUserTimePasswordFormatBranches(t *testing.T) {
	stmt, err := Parse("alter user u1 password_life_time 2 password_reuse_time 3 password_reuse_max 4 password_lock_time 5 password_grace_time 6;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil {
		t.Fatalf("expected user options")
	}
	if !au.UserOptions.HasPasswordLifeTime || !au.UserOptions.HasPasswordReuseTime || !au.UserOptions.HasPasswordReuseMax || !au.UserOptions.HasPasswordLockTime || !au.UserOptions.HasPasswordGraceTime {
		t.Fatalf("expected all password/time flags set: %+v", au.UserOptions)
	}
	got := formatStatementForRoundTrip(t, au)
	if got == "" {
		t.Fatalf("expected non-empty format")
	}
}

func TestUserSQLCoverage_DateTimeRangeMonthUnit(t *testing.T) {
	stmt, err := Parse("create user u_month pass 'p' allow_datetime '1n';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	cu, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}
	if len(cu.TimeRanges) == 0 {
		t.Fatalf("expected allow_datetime ranges, got %+v", cu)
	}
}

func TestUserSQLCoverage_AlterUserInactiveAccountTimeFormat(t *testing.T) {
	stmt, err := Parse("alter user u1 inactive_account_time 7;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil || !au.UserOptions.HasInactiveAccountTime {
		t.Fatalf("expected inactive_account_time option, got %+v", au.UserOptions)
	}
	got := formatStatementForRoundTrip(t, au)
	if got != "alter user u1 inactive_account_time 7" {
		t.Fatalf("unexpected formatted sql: %q", got)
	}
}

func TestUserSQLCoverage_AlterUserDropMergePaths(t *testing.T) {
	stmt, err := Parse("alter user u1 drop not_allow_host '127.0.0.1/32' drop not_allow_datetime '1d';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	au, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}
	if au.UserOptions == nil {
		t.Fatalf("expected user options")
	}
	if len(au.UserOptions.DropIpRanges) == 0 || len(au.UserOptions.DropTimeRanges) == 0 {
		t.Fatalf("expected both drop lists, got %+v", au.UserOptions)
	}
	if au.UserOptions.DropIpRanges[0].Neg != 1 || au.UserOptions.DropTimeRanges[0].Neg != 1 {
		t.Fatalf("expected negation on drop lists, got %+v", au.UserOptions)
	}
}

func TestUserSQLCoverage_CreateUserInvalidValues(t *testing.T) {
	invalid := []string{
		"create user u1 pass 'p' enable x;",
		"create user u1 pass 'p' session_per_user x;",
		"create user u1 pass 'p' connect_time x;",
		"create user u1 pass 'p' password_reuse_time unlimited;",
		"create user u1 pass 'p' password_reuse_max unlimited;",
		"create user u1 pass 'p' allow_token_num x;",
		"create user u1 pass 'p' host 'bad-cidr';",
		"create user u1 pass 'p' allow_datetime 'bad';",
	}
	for _, sql := range invalid {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected parse error for %q", sql)
		}
	}
}
