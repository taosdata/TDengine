package sqlparser

import "testing"

func TestValidSQLRoundTrip_BacklogCases(t *testing.T) {
	cases := []struct {
		id  string
		sql string
	}{
		{
			id:  "v0062",
			sql: "alter table db1.t1 add column c2 int;",
		},
		{
			id:  "v0248",
			sql: "create table if not exists db1.ta using db1.st1 tags(1) if not exists db1.tb using db1.st1 tags(2);",
		},
		{
			id:  "v0261",
			sql: "create vtable if not exists db1.vt2 using db1.st1 tags (1, 'x');",
		},
		{
			id:  "v0262",
			sql: "create xnode 'xn1';",
		},
		{
			id:  "v0316",
			sql: "recalculate stream db1.s1 from 1 to '2020-01-01';",
		},
		{
			id:  "v0553",
			sql: "create user ux pass 'p' session_per_user unlimited connect_time unlimited connect_idle_time unlimited call_per_session unlimited vnode_per_call unlimited failed_login_attempts unlimited password_life_time unlimited password_lock_time unlimited password_grace_time unlimited inactive_account_time unlimited allow_token_num unlimited;",
		},
		{
			id:  "v0555",
			sql: "create user uhost pass 'p' host '127.0.0.1/32' allow_datetime '2024-01-02 03:04:05';",
		},
		{
			id:  "v0556",
			sql: "create user uhost2 pass 'p' not_allow_host '10.0.0.0/24' not_allow_datetime '2024-01-02 03:04';",
		},
		{
			id:  "v0557",
			sql: "create user uhost3 pass 'p' allow_datetime '2024-01-02';",
		},
		{
			id:  "v0565",
			sql: "create table if not exists db1.tcov (ts timestamp, v int) max_delay 1s,2s watermark 1d,2d delete_mark 3d keep 4d;",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.id, func(t *testing.T) {
			runStatementRoundTrip(t, tc.sql)
		})
	}
}
