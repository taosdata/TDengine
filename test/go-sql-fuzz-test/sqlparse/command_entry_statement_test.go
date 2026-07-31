package sqlparser

import "testing"

func TestCommandEntry_StatementMatrix(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		check func(t *testing.T, stmt Statement)
	}{
		{
			name: "create user",
			sql:  "create user u_cmd pass 'p' enable 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateUserStmt)
				if !ok {
					t.Fatalf("expected *CreateUserStmt, got %T", stmt)
				}
				if s.UserName != "u_cmd" {
					t.Fatalf("unexpected user: %+v", s)
				}
			},
		},
		{
			name: "alter user",
			sql:  "alter user u_cmd enable 0;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterUserStmt)
				if !ok {
					t.Fatalf("expected *AlterUserStmt, got %T", stmt)
				}
				if s.UserName != "u_cmd" {
					t.Fatalf("unexpected user: %+v", s)
				}
			},
		},
		{
			name: "drop user",
			sql:  "drop user if exists u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropUserStmt)
				if !ok {
					t.Fatalf("expected *DropUserStmt, got %T", stmt)
				}
				if s.UserName != "u_cmd" {
					t.Fatalf("unexpected user: %+v", s)
				}
			},
		},
		{
			name: "create token",
			sql:  "create token tk_cmd from user u_cmd ttl 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTokenStmt)
				if !ok {
					t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
				}
				if s.Name != "tk_cmd" {
					t.Fatalf("unexpected token: %+v", s)
				}
			},
		},
		{
			name: "alter token",
			sql:  "alter token tk_cmd enable 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTokenStmt)
				if !ok {
					t.Fatalf("expected *AlterTokenStmt, got %T", stmt)
				}
				if s.Name != "tk_cmd" {
					t.Fatalf("unexpected token: %+v", s)
				}
			},
		},
		{
			name: "drop token",
			sql:  "drop token if exists tk_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropTokenStmt)
				if !ok {
					t.Fatalf("expected *DropTokenStmt, got %T", stmt)
				}
				if s.Name != "tk_cmd" {
					t.Fatalf("unexpected token: %+v", s)
				}
			},
		},
		{
			name: "create role",
			sql:  "create role if not exists r_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateRoleStmt)
				if !ok {
					t.Fatalf("expected *CreateRoleStmt, got %T", stmt)
				}
				if s.Name != "r_cmd" {
					t.Fatalf("unexpected role: %+v", s)
				}
			},
		},
		{
			name: "drop role",
			sql:  "drop role if exists r_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropRoleStmt)
				if !ok {
					t.Fatalf("expected *DropRoleStmt, got %T", stmt)
				}
				if s.Name != "r_cmd" {
					t.Fatalf("unexpected role: %+v", s)
				}
			},
		},
		{
			name: "create function",
			sql:  "create aggregate function if not exists f_cmd as 'return 1' outputtype int bufsize 16 language 'python';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateFunctionStmt)
				if !ok {
					t.Fatalf("expected *CreateFunctionStmt, got %T", stmt)
				}
				if s.Name != "f_cmd" || s.Body != "return 1" || s.OutputType != "int" || !s.Aggregate || !s.IgnoreExists || s.Bufsize != 16 || s.Language != "python" {
					t.Fatalf("unexpected create function stmt: %+v", s)
				}
			},
		},
		{
			name: "create stream",
			sql:  "create stream if not exists s_cmd session(ts, 10s) into db1.tout (c1) as select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*StreamStmt)
				if !ok {
					t.Fatalf("expected *StreamStmt, got %T", stmt)
				}
				if s.Action != "create" || !s.NotExists || len(s.Names) != 1 || s.Names[0] != "s_cmd" || s.Query == nil {
					t.Fatalf("unexpected create stream stmt: %+v", s)
				}
			},
		},
		{
			name: "lock role",
			sql:  "lock role r_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterRoleStmt)
				if !ok {
					t.Fatalf("expected *AlterRoleStmt, got %T", stmt)
				}
				if s.Name != "r_cmd" || s.Action != TSDB_ALTER_ROLE_LOCK {
					t.Fatalf("unexpected role alter: %+v", s)
				}
			},
		},
		{
			name: "grant role",
			sql:  "grant role r_cmd to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantRoleStmt)
				if !ok {
					t.Fatalf("expected *GrantRoleStmt, got %T", stmt)
				}
				if s.RoleName != "r_cmd" || s.GranteeName != "u_cmd" {
					t.Fatalf("unexpected grant role stmt: %+v", s)
				}
			},
		},
		{
			name: "revoke role",
			sql:  "revoke role r_cmd from u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*RevokeRoleStmt)
				if !ok {
					t.Fatalf("expected *RevokeRoleStmt, got %T", stmt)
				}
				if s.RoleName != "r_cmd" || s.RevokeeName != "u_cmd" {
					t.Fatalf("unexpected revoke role stmt: %+v", s)
				}
			},
		},
		{
			name: "grant all privileges",
			sql:  "grant all to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_CM_ALL || s.PrivilegeName != "all" {
					t.Fatalf("unexpected grant all stmt: %+v", s)
				}
			},
		},
		{
			name: "revoke all privileges",
			sql:  "revoke all from u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 1 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_CM_ALL || s.PrivilegeName != "all" {
					t.Fatalf("unexpected revoke all stmt: %+v", s)
				}
			},
		},
		{
			name: "grant read",
			sql:  "grant read to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || s.PrivilegeName != "read" {
					t.Fatalf("unexpected grant read stmt: %+v", s)
				}
			},
		},
		{
			name: "grant alter",
			sql:  "grant alter to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_CM_ALTER || s.PrivilegeName != "alter" {
					t.Fatalf("unexpected grant alter stmt: %+v", s)
				}
			},
		},
		{
			name: "grant show create",
			sql:  "grant show create to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_CM_SHOW_CREATE || s.PrivilegeName != "show create" {
					t.Fatalf("unexpected grant show create stmt: %+v", s)
				}
			},
		},
		{
			name: "grant create database",
			sql:  "grant create database to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_DB_CREATE || s.PrivilegeName != "create database" {
					t.Fatalf("unexpected grant create database stmt: %+v", s)
				}
			},
		},
		{
			name: "grant on database",
			sql:  "grant create table on database db_cmd to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.Privileges.ObjType != PRIV_OBJ_DB || s.ObjName != "db_cmd" || s.PrivilegeName != "create table" {
					t.Fatalf("unexpected grant on database stmt: %+v", s)
				}
			},
		},
		{
			name: "grant privilege list",
			sql:  "grant read, write, alter to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.PrivilegeName != "read, write, alter" {
					t.Fatalf("unexpected grant list stmt: %+v", s)
				}
			},
		},
		{
			name: "grant wildcard level",
			sql:  "grant show on *.* to u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 0 || s.Principal != "u_cmd" || s.ObjName != "*" || s.TabName != "*" || s.Privileges.ObjType != PRIV_OBJ_TBL {
					t.Fatalf("unexpected grant wildcard stmt: %+v", s)
				}
			},
		},
		{
			name: "revoke write",
			sql:  "revoke write from u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 1 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_TYPE_UNKNOWN || s.PrivilegeName != "write" {
					t.Fatalf("unexpected revoke write stmt: %+v", s)
				}
			},
		},
		{
			name: "revoke on table",
			sql:  "revoke drop on table db_cmd.t1 from u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 1 || s.Principal != "u_cmd" || s.Privileges.ObjType != PRIV_OBJ_TBL || s.ObjName != "db_cmd" || s.TabName != "t1" || s.PrivilegeName != "drop" {
					t.Fatalf("unexpected revoke on table stmt: %+v", s)
				}
			},
		},
		{
			name: "revoke alter",
			sql:  "revoke alter from u_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*GrantStmt)
				if !ok {
					t.Fatalf("expected *GrantStmt, got %T", stmt)
				}
				if s.OptrType != 1 || s.Principal != "u_cmd" || s.Privileges.PrivArgs != PRIV_CM_ALTER || s.PrivilegeName != "alter" {
					t.Fatalf("unexpected revoke alter stmt: %+v", s)
				}
			},
		},
		{
			name: "create encrypt key",
			sql:  "create encrypt_key 'k';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateEncryptKeyStmt); !ok {
					t.Fatalf("expected *CreateEncryptKeyStmt, got %T", stmt)
				}
			},
		},
		{
			name: "alter encrypt key",
			sql:  "alter system set db_key 'k';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterEncryptKeyStmt); !ok {
					t.Fatalf("expected *AlterEncryptKeyStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create encrypt algr",
			sql:  "create encrypt_algr 'a' algr_name 'n' desc 'd' algr_type 't' ossl_algr_name 'o';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateAlgrStmt); !ok {
					t.Fatalf("expected *CreateAlgrStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop encrypt algr",
			sql:  "drop encrypt_algr 'a';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropAlgrStmt); !ok {
					t.Fatalf("expected *DropAlgrStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create anode",
			sql:  "create anode 'ep';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateAnodeStmt); !ok {
					t.Fatalf("expected *CreateAnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "update anode",
			sql:  "update anode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*UpdateAnodeStmt); !ok {
					t.Fatalf("expected *UpdateAnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop anode",
			sql:  "drop anode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropAnodeStmt); !ok {
					t.Fatalf("expected *DropAnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create dnode",
			sql:  "create dnode 'n1' port 6030;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateDnodeStmt); !ok {
					t.Fatalf("expected *CreateDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop dnode",
			sql:  "drop dnode 1 force;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropDnodeStmt); !ok {
					t.Fatalf("expected *DropDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "alter dnode",
			sql:  "alter dnode 1 'cfg' 'v';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterDnodeStmt); !ok {
					t.Fatalf("expected *AlterDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "restore dnode",
			sql:  "restore dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*RestoreDnodeStmt); !ok {
					t.Fatalf("expected *RestoreDnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "alter dnodes reload",
			sql:  "alter dnodes reload tls;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterDnodesReloadStmt)
				if !ok {
					t.Fatalf("expected *AlterDnodesReloadStmt, got %T", stmt)
				}
				if s.Name != "tls" {
					t.Fatalf("unexpected reload stmt: %+v", s)
				}
			},
		},
		{
			name: "alter cluster",
			sql:  "alter cluster 'k' 'v';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterClusterStmt); !ok {
					t.Fatalf("expected *AlterClusterStmt, got %T", stmt)
				}
			},
		},
		{
			name: "alter local",
			sql:  "alter local 'k' 'v';",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AlterLocalStmt); !ok {
					t.Fatalf("expected *AlterLocalStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create qnode",
			sql:  "create qnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop qnode",
			sql:  "drop qnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropComponentNodeStmt); !ok {
					t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "restore qnode",
			sql:  "restore qnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
					t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create snode",
			sql:  "create snode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop snode",
			sql:  "drop snode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropComponentNodeStmt); !ok {
					t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create bnode",
			sql:  "create bnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateBnodeStmt); !ok {
					t.Fatalf("expected *CreateBnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop bnode",
			sql:  "drop bnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropBnodeStmt); !ok {
					t.Fatalf("expected *DropBnodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create mnode",
			sql:  "create mnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
					t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "drop mnode",
			sql:  "drop mnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DropComponentNodeStmt); !ok {
					t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "restore mnode",
			sql:  "restore mnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
					t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "restore vnode",
			sql:  "restore vnode on dnode 1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
					t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create database",
			sql:  "create database if not exists db_cmd buffer 10;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected db: %+v", s)
				}
			},
		},
		{
			name: "alter database option chain",
			sql:  "alter database db_cmd wal_retention_period -3 ss_keeplocal 2 compact_interval 30;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterDatabaseStmt)
				if !ok {
					t.Fatalf("expected *AlterDatabaseStmt, got %T", stmt)
				}
				if s.Name != "db_cmd" || s.Options == nil {
					t.Fatalf("unexpected alter database stmt: %+v", s)
				}
				if s.Options.WalRetentionPeriod != -3 || !s.Options.WalRetentionPeriodIsSet || s.Options.SsKeepLocal != 2 || s.Options.CompactInterval != 30 {
					t.Fatalf("unexpected alter database options: %+v", s.Options)
				}
			},
		},
		{
			name: "drop database",
			sql:  "drop database if exists db_cmd force;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropDatabaseStmt)
				if !ok {
					t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected db: %+v", s)
				}
			},
		},
		{
			name: "use database",
			sql:  "use db_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*UseDatabaseStmt)
				if !ok {
					t.Fatalf("expected *UseDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected db: %+v", s)
				}
			},
		},
		{
			name: "flush database",
			sql:  "flush database db_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*FlushDatabaseStmt)
				if !ok {
					t.Fatalf("expected *FlushDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected flush db stmt: %+v", s)
				}
			},
		},
		{
			name: "ssmigrate database",
			sql:  "ssmigrate database db_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*SsMigrateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *SsMigrateDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected ssmigrate stmt: %+v", s)
				}
			},
		},
		{
			name: "trim database",
			sql:  "trim database db_cmd bwlimit 5;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*TrimDatabaseStmt)
				if !ok {
					t.Fatalf("expected *TrimDatabaseStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" || s.BwLimit != 5 {
					t.Fatalf("unexpected trim database stmt: %+v", s)
				}
			},
		},
		{
			name: "trim database wal",
			sql:  "trim database db_cmd wal;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*TrimDatabaseWalStmt)
				if !ok {
					t.Fatalf("expected *TrimDatabaseWalStmt, got %T", stmt)
				}
				if s.DbName != "db_cmd" {
					t.Fatalf("unexpected trim wal stmt: %+v", s)
				}
			},
		},
		{
			name: "kill connection",
			sql:  "kill connection 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*KillStmt)
				if !ok {
					t.Fatalf("expected *KillStmt, got %T", stmt)
				}
				if s.Kind != "connection" || s.Target != "1" {
					t.Fatalf("unexpected kill stmt: %+v", s)
				}
			},
		},
		{
			name: "show xnodes",
			sql:  "show xnodes;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "xnodes" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show dnodes",
			sql:  "show dnodes;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "dnodes" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show users",
			sql:  "show users;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "users" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show users full",
			sql:  "show users full;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "users_full" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show user privileges",
			sql:  "show user privileges;",
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
			name: "show roles",
			sql:  "show roles;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "roles" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show role privileges",
			sql:  "show role privileges;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "role_privileges" {
					t.Fatalf("unexpected show stmt: %+v", s)
				}
			},
		},
		{
			name: "show role column privileges",
			sql:  "show role column privileges;",
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
			name: "show apps",
			sql:  "show apps;",
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
			name: "show connections",
			sql:  "show connections;",
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
			name: "show licences",
			sql:  "show licences;",
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
			name: "show grants",
			sql:  "show grants;",
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
			name: "show grants full",
			sql:  "show grants full;",
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
			name: "show grants logs",
			sql:  "show grants logs;",
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
			name: "show encryptions",
			sql:  "show encryptions;",
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
			name: "show encrypt algorithms",
			sql:  "show encrypt_algorithms;",
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
			name: "show encrypt status",
			sql:  "show encrypt_status;",
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
			name: "show queries",
			sql:  "show queries;",
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
			name: "show scores",
			sql:  "show scores;",
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
			name: "show topics",
			sql:  "show topics;",
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
			name: "show consumers",
			sql:  "show consumers;",
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
			name: "show subscriptions",
			sql:  "show subscriptions;",
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
			name: "show tokens",
			sql:  "show tokens;",
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
			name: "show snodes",
			sql:  "show snodes;",
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
			name: "balance vgroup",
			sql:  "balance vgroup;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*BalanceVgroupStmt); !ok {
					t.Fatalf("expected *BalanceVgroupStmt, got %T", stmt)
				}
			},
		},
		{
			name: "balance vgroup leader database",
			sql:  "balance vgroup leader database db_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*BalanceVgroupLeaderStmt)
				if !ok {
					t.Fatalf("expected *BalanceVgroupLeaderStmt, got %T", stmt)
				}
				if s.Database != "db_cmd" {
					t.Fatalf("unexpected balance vgroup leader stmt: %+v", s)
				}
			},
		},
		{
			name: "create index",
			sql:  "create index if not exists idx_cmd on db_cmd.t_cmd(v1, v2);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "index" || !s.IfNotExists || s.Name != "idx_cmd" || s.OnTable != "db_cmd.t_cmd" {
					t.Fatalf("unexpected create index stmt: %+v", s)
				}
			},
		},
		{
			name: "create sma index",
			sql:  "create sma index if not exists idxs_cmd on db_cmd.t_cmd function(avg(v1)) interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "sma_index" || !s.IfNotExists || s.Name != "idxs_cmd" || s.OnTable != "db_cmd.t_cmd" {
					t.Fatalf("unexpected create sma index stmt: %+v", s)
				}
			},
		},
		{
			name: "create rsma",
			sql:  "create rsma if not exists rsma_cmd on db_cmd.t_cmd function(avg(v1)) interval(1d, 2d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || !s.IfNotExists || s.Name != "rsma_cmd" || s.OnTable != "db_cmd.t_cmd" {
					t.Fatalf("unexpected create rsma stmt: %+v", s)
				}
			},
		},
		{
			name: "create tsma",
			sql:  "create tsma if not exists tsma_cmd on db_cmd.t_cmd function(avg(v1)) interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || s.IsRecursive || !s.IfNotExists || s.Name != "tsma_cmd" || s.OnTable != "db_cmd.t_cmd" {
					t.Fatalf("unexpected create tsma stmt: %+v", s)
				}
			},
		},
		{
			name: "create recursive tsma",
			sql:  "create recursive tsma if not exists tsma_rec_cmd on db_cmd.t_cmd interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || !s.IsRecursive || !s.IfNotExists || s.Name != "tsma_rec_cmd" || s.OnTable != "db_cmd.t_cmd" {
					t.Fatalf("unexpected create recursive tsma stmt: %+v", s)
				}
			},
		},
		{
			name: "create table",
			sql:  "create table db_cmd.t_cmd(ts timestamp, v int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.TableName == nil || s.TableName.Name.String() != "t_cmd" || s.Options == nil {
					t.Fatalf("unexpected table: %+v", s)
				}
			},
		},
		{
			name: "create stable",
			sql:  "create stable db_cmd.st_cmd(ts timestamp, v int) tags(tag1 int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.TableName == nil || s.TableName.Name.String() != "st_cmd" || !s.IsStable || len(s.Tags) != 1 || s.Options == nil {
					t.Fatalf("unexpected stable: %+v", s)
				}
			},
		},
		{
			name: "select statement",
			sql:  "select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*SelectStmt); !ok {
					t.Fatalf("expected *SelectStmt, got %T", stmt)
				}
			},
		},
		{
			name: "explain statement",
			sql:  "explain select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ExplainStmt); !ok {
					t.Fatalf("expected *ExplainStmt, got %T", stmt)
				}
			},
		},
		{
			name: "create view",
			sql:  "create view v_cmd as select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*CreateViewStmt); !ok {
					t.Fatalf("expected *CreateViewStmt, got %T", stmt)
				}
			},
		},
		{
			name: "assign leader force",
			sql:  "assign leader force;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*AssignLeaderStmt); !ok {
					t.Fatalf("expected *AssignLeaderStmt, got %T", stmt)
				}
			},
		},
		{
			name: "alter vgroup set keep",
			sql:  "alter vgroup 7 set keep 3;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterVgroupKeepStmt)
				if !ok {
					t.Fatalf("expected *AlterVgroupKeepStmt, got %T", stmt)
				}
				if s.VgroupID != 7 || s.Keep != 3 {
					t.Fatalf("unexpected alter vgroup keep stmt: %+v", s)
				}
			},
		},
		{
			name: "merge vgroup",
			sql:  "merge vgroup 3 9;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*MergeVgroupStmt)
				if !ok {
					t.Fatalf("expected *MergeVgroupStmt, got %T", stmt)
				}
				if s.SourceVgroupID != 3 || s.TargetVgroupID != 9 {
					t.Fatalf("unexpected merge vgroup stmt: %+v", s)
				}
			},
		},
		{
			name: "split vgroup force",
			sql:  "split vgroup 8 force;",
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
			name: "redistribute vgroup",
			sql:  "redistribute vgroup 9 dnode 1 dnode 2;",
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
			name: "create topic",
			sql:  "create topic if not exists tp_cmd as select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*TopicStmt); !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
			},
		},
		{
			name: "reload topic",
			sql:  "reload topic if exists tp_cmd as select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*TopicStmt)
				if !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
				if !s.Reload {
					t.Fatalf("expected reload topic stmt, got %+v", s)
				}
			},
		},
		{
			name: "drop topic",
			sql:  "drop topic if exists force tp_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*TopicStmt)
				if !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
				if !s.Drop {
					t.Fatalf("expected drop topic stmt, got %+v", s)
				}
			},
		},
		{
			name: "drop consumer group",
			sql:  "drop consumer group if exists force cg_cmd on tp_cmd;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*TopicStmt)
				if !ok {
					t.Fatalf("expected *TopicStmt, got %T", stmt)
				}
				if !s.DropGroup {
					t.Fatalf("expected drop consumer group stmt, got %+v", s)
				}
			},
		},
		{
			name: "describe",
			sql:  "describe t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*DescribeStmt); !ok {
					t.Fatalf("expected *DescribeStmt, got %T", stmt)
				}
			},
		},
		{
			name: "reset query cache",
			sql:  "reset query cache;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*ResetQueryCacheStmt); !ok {
					t.Fatalf("expected *ResetQueryCacheStmt, got %T", stmt)
				}
			},
		},
		{
			name: "insert query",
			sql:  "insert into t2 select v from t1;",
			check: func(t *testing.T, stmt Statement) {
				if _, ok := stmt.(*InsertQueryStmt); !ok {
					t.Fatalf("expected *InsertQueryStmt, got %T", stmt)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse failed for %q: %v", tc.sql, err)
			}
			tc.check(t, stmt)
		})
	}
}
