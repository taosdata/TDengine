package sqlparser

import "testing"

func TestCommandMatrix_Parse(t *testing.T) {
	okSQL := []struct {
		sql   string
		check func(t *testing.T, stmt Statement)
	}{
		{
			sql: "create token tk from user u ttl 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateTokenStmt); !ok {
					t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "alter token tk provider 'x';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterTokenStmt); !ok {
					t.Fatalf("expected *AlterTokenStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create user u pass 'p' allow_token_num 2;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateUserStmt); !ok {
					t.Fatalf("expected *CreateUserStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "alter user u add host '127.0.0.1/32';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterUserStmt); !ok {
					t.Fatalf("expected *AlterUserStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "drop user if exists u;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropUserStmt); !ok {
					t.Fatalf("expected *DropUserStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create aggregate function if not exists f as 'return 1' outputtype int bufsize 8 language 'python';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateFunctionStmt)
				if !ok {
					t.Fatalf("expected *CreateFunctionStmt, got %T", stmt)
				}
				if s.Name != "f" || s.OutputType != "int" || !s.Aggregate || !s.IgnoreExists || s.Bufsize != 8 || s.Language != "python" {
					t.Fatalf("unexpected create function stmt: %+v", s)
				}
			},
		},
		{
			sql: "create dnode n1 port 6030;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateDnodeStmt); !ok {
					t.Fatalf("expected *CreateDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "drop dnode n1 unsafe;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropDnodeStmt); !ok {
					t.Fatalf("expected *DropDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "alter all dnodes 'k' 'v';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterDnodeStmt); !ok {
					t.Fatalf("expected *AlterDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "alter dnodes reload tls;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterDnodesReloadStmt); !ok {
					t.Fatalf("expected *AlterDnodesReloadStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create qnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create snode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create bnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateBnodeStmt); !ok {
					t.Fatalf("expected *CreateBnodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create mnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "restore vnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
					t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create database if not exists db buffer 10 cachemodel 'none' precision 'ms' wal_level 1 retentions 1d:30d;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateDatabaseStmt); !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create table db.t(ts timestamp, v int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.TableName == nil || s.TableName.Name.String() != "t" || s.Options == nil {
					t.Fatalf("unexpected create table stmt: %+v", s)
				}
			},
		},
		{
			sql: "create stable db.st(ts timestamp, v int) tags(tag1 int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.TableName == nil || s.TableName.Name.String() != "st" || !s.IsStable || len(s.Tags) != 1 || s.Options == nil {
					t.Fatalf("unexpected create stable stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter database db wal_retention_period -3 ss_keeplocal 2 compact_time_range -1,2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterDatabaseStmt)
				if !ok {
					t.Fatalf("expected *AlterDatabaseStmt, got %T", stmt)
				}
				if s.Name != "db" || s.Options == nil {
					t.Fatalf("unexpected alter database stmt: %+v", s)
				}
				if s.Options.WalRetentionPeriod != -3 || !s.Options.WalRetentionPeriodIsSet || s.Options.SsKeepLocal != 2 || s.Options.CompactStartTime != -1 || s.Options.CompactEndTime != 2 {
					t.Fatalf("unexpected alter database options: %+v", s.Options)
				}
			},
		},
		{
			sql: "drop database if exists db force;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropDatabaseStmt); !ok {
					t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "flush database db;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*FlushDatabaseStmt); !ok {
					t.Fatalf("expected *FlushDatabaseStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "ssmigrate database db;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*SsMigrateDatabaseStmt); !ok {
					t.Fatalf("expected *SsMigrateDatabaseStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "trim database db bwlimit 5;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*TrimDatabaseStmt); !ok {
					t.Fatalf("expected *TrimDatabaseStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "trim database db wal;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*TrimDatabaseWalStmt); !ok {
					t.Fatalf("expected *TrimDatabaseWalStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create stream if not exists s1 session(ts, 10s) into db1.tout (c1) as select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*StreamStmt)
				if !ok {
					t.Fatalf("expected *StreamStmt, got %T", stmt)
				}
				if s.Action != "create" || !s.NotExists || len(s.Names) != 1 || s.Names[0] != "s1" || s.Query == nil {
					t.Fatalf("unexpected create stream stmt: %+v", s)
				}
			},
		},
		{
			sql: "kill connection 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*KillStmt); !ok {
					t.Fatalf("expected *KillStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show xnodes;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show dnodes;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show users;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show users full;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show user privileges;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "user_privileges" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show roles;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show role privileges;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ShowStmt); !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "show role column privileges;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "role_column_privileges" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show apps;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "apps" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show connections;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "connections" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show licences;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "licences" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show grants;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "grants" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show grants full;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "grants_full" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show grants logs;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "grants_logs" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show encryptions;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "encryptions" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show encrypt_algorithms;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "encrypt_algorithms" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show encrypt_status;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "encrypt_status" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show queries;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "queries" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show scores;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "scores" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show topics;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "topics" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show consumers;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "consumers" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show subscriptions;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "subscriptions" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show tokens;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "tokens" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "show snodes;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "snodes" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			sql: "balance vgroup;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*BalanceVgroupStmt); !ok {
					t.Fatalf("expected *BalanceVgroupStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "balance vgroup leader database db;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*BalanceVgroupLeaderStmt); !ok {
					t.Fatalf("expected *BalanceVgroupLeaderStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "create index if not exists idx on db.t(v1, v2);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "index" || !s.IfNotExists || s.Name != "idx" || s.OnTable != "db.t" {
					t.Fatalf("unexpected create index stmt: %+v", s)
				}
			},
		},
		{
			sql: "create rsma if not exists r1 on db.t function(avg(v1)) interval(1d, 2d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || !s.IfNotExists || s.Name != "r1" || s.OnTable != "db.t" {
					t.Fatalf("unexpected create rsma stmt: %+v", s)
				}
			},
		},
		{
			sql: "create recursive tsma if not exists t1 on db.t interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || !s.IsRecursive || !s.IfNotExists || s.Name != "t1" || s.OnTable != "db.t" {
					t.Fatalf("unexpected create recursive tsma stmt: %+v", s)
				}
			},
		},
		{
			sql: "assign leader force;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AssignLeaderStmt); !ok {
					t.Fatalf("expected *AssignLeaderStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "alter vgroup 7 set keep 3;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterVgroupKeepStmt); !ok {
					t.Fatalf("expected *AlterVgroupKeepStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "merge vgroup 3 9;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*MergeVgroupStmt); !ok {
					t.Fatalf("expected *MergeVgroupStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "split vgroup 8 force;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*SplitVgroupStmt)
				if !ok {
					t.Fatalf("expected *SplitVgroupStmt, got %T", stmt)
				}
				if s.VgroupID != 8 || !s.Force {
					t.Fatalf("unexpected split vgroup stmt: %+v", s)
				}
			},
		},
		{
			sql: "redistribute vgroup 9 dnode 1 dnode 2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*RedistributeVgroupStmt)
				if !ok {
					t.Fatalf("expected *RedistributeVgroupStmt, got %T", stmt)
				}
				if s.VgroupID != 9 || len(s.DnodeIDs) != 2 || s.DnodeIDs[0] != 1 || s.DnodeIDs[1] != 2 {
					t.Fatalf("unexpected redistribute vgroup stmt: %+v", s)
				}
			},
		},
		{
			sql: "create topic if not exists tp as select v from t;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*TopicStmt); !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "drop consumer group if exists force cg on tp;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*TopicStmt); !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
			},
		},
		{
			sql: "grant read to u;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u" || s.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || s.PrivilegeName != "read" {
					t.Fatalf("unexpected grant stmt: %+v", s)
				}
			},
		},
	}
	for _, tc := range okSQL {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("expected parse success for %q, got %v", tc.sql, err)
		}
		tc.check(t, stmt)
	}

	// STRICT is commented out in lemon/sql.y and intentionally unsupported.
	if _, err := Parse("create database if not exists db strict 1;"); err == nil {
		t.Fatalf("expected strict option parse error")
	}
}
