package sqlparser

import "testing"

func TestStatementRuntimeContracts_ZeroValues(t *testing.T) {
	statements := []Statement{
		&AlterClusterStmt{},
		&AlterDatabaseStmt{},
		&AlterDnodeStmt{},
		&AlterDnodesReloadStmt{},
		&AlterEncryptKeyStmt{},
		&AlterLocalStmt{},
		&AlterNamedStmt{},
		&AlterRoleStmt{},
		&AlterTableStmt{},
		&AlterTokenStmt{},
		&AlterUserStmt{},
		&AlterVgroupKeepStmt{},
		&AssignLeaderStmt{},
		&BalanceVgroupLeaderStmt{},
		&BalanceVgroupStmt{},
		&CompactStmt{},
		&CreateAnodeStmt{},
		&CreateBnodeStmt{},
		&CreateComponentNodeStmt{},
		&CreateDatabaseStmt{},
		&CreateDnodeStmt{},
		&CreateEncryptAlgrStmt{},
		&CreateMountStmt{},
		&CreateNamedStmt{},
		&CreateRoleStmt{},
		&CreateSubTableFromFileStmt{},
		&CreateTableStmt{},
		&CreateTokenStmt{},
		&CreateUserStmt{},
		&CreateViewStmt{},
		&CreateVSubTableStmt{},
		&DeleteStmt{},
		&DescribeStmt{},
		&DropAnodeStmt{},
		&DropBnodeStmt{},
		&DropComponentNodeStmt{},
		&DropDatabaseStmt{},
		&DropDnodeStmt{},
		&DropEncryptAlgrStmt{},
		&CreateFunctionStmt{},
		&DropFunctionStmt{},
		&DropMountStmt{},
		&DropNamedStmt{},
		&DropRoleStmt{},
		&DropTableStmt{},
		&DropTokenStmt{},
		&DropUserStmt{},
		&DropViewStmt{},
		&ExplainStmt{},
		&FlushDatabaseStmt{},
		&GrantRoleStmt{},
		&GrantStmt{},
		&InsertQueryStmt{},
		InsertStatement{},
		&KillStmt{},
		&MergeVgroupStmt{},
		&MultiCreateTableStmt{},
		&RedistributeVgroupStmt{},
		&ResetQueryCacheStmt{},
		&RestoreComponentNodeStmt{},
		&RestoreDnodeStmt{},
		&RevokeRoleStmt{},
		&RollupStmt{},
		&ScanStmt{},
		&SelectStmt{},
		&ShowStmt{},
		&SplitVgroupStmt{},
		&SsMigrateDatabaseStmt{},
		&StreamStmt{},
		&TopicStmt{},
		&TrimDatabaseStmt{},
		&TrimDatabaseWalStmt{},
		&UpdateAnodeStmt{},
		&UseDatabaseStmt{},
		&XnodeStmt{},
	}

	for _, stmt := range statements {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic on zero-value statement %T: %v", stmt, r)
				}
			}()
			tb := newTB()
			stmt.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic on zero-value statement %T: %v", stmt, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, stmt); err != nil {
				t.Fatalf("Walk failed on zero-value statement %T: %v", stmt, err)
			}
		}()
	}
}

func TestStatementRuntimeContracts_NilPointers(t *testing.T) {
	var (
		sAlterCluster           *AlterClusterStmt
		sAlterDatabase          *AlterDatabaseStmt
		sAlterDnode             *AlterDnodeStmt
		sAlterDnodesReload      *AlterDnodesReloadStmt
		sAlterEncryptKey        *AlterEncryptKeyStmt
		sAlterLocal             *AlterLocalStmt
		sAlterNamed             *AlterNamedStmt
		sAlterRole              *AlterRoleStmt
		sAlterTable             *AlterTableStmt
		sAlterToken             *AlterTokenStmt
		sAlterUser              *AlterUserStmt
		sAlterVgroupKeep        *AlterVgroupKeepStmt
		sAssignLeader           *AssignLeaderStmt
		sBalanceVgroupLeader    *BalanceVgroupLeaderStmt
		sBalanceVgroup          *BalanceVgroupStmt
		sCompact                *CompactStmt
		sCreateAnode            *CreateAnodeStmt
		sCreateBnode            *CreateBnodeStmt
		sCreateComponentNode    *CreateComponentNodeStmt
		sCreateDatabase         *CreateDatabaseStmt
		sCreateDnode            *CreateDnodeStmt
		sCreateEncryptAlgr      *CreateEncryptAlgrStmt
		sCreateMount            *CreateMountStmt
		sCreateNamed            *CreateNamedStmt
		sCreateRole             *CreateRoleStmt
		sCreateSubTableFromFile *CreateSubTableFromFileStmt
		sCreateTable            *CreateTableStmt
		sCreateToken            *CreateTokenStmt
		sCreateUser             *CreateUserStmt
		sCreateView             *CreateViewStmt
		sCreateVSubTable        *CreateVSubTableStmt
		sDelete                 *DeleteStmt
		sDescribe               *DescribeStmt
		sDropAnode              *DropAnodeStmt
		sDropBnode              *DropBnodeStmt
		sDropComponentNode      *DropComponentNodeStmt
		sDropDatabase           *DropDatabaseStmt
		sDropDnode              *DropDnodeStmt
		sDropEncryptAlgr        *DropEncryptAlgrStmt
		sCreateFunction         *CreateFunctionStmt
		sDropFunction           *DropFunctionStmt
		sDropMount              *DropMountStmt
		sDropNamed              *DropNamedStmt
		sDropRole               *DropRoleStmt
		sDropTable              *DropTableStmt
		sDropToken              *DropTokenStmt
		sDropUser               *DropUserStmt
		sDropView               *DropViewStmt
		sExplain                *ExplainStmt
		sFlushDatabase          *FlushDatabaseStmt
		sGrantRole              *GrantRoleStmt
		sGrant                  *GrantStmt
		sInsertQuery            *InsertQueryStmt
		sKill                   *KillStmt
		sMergeVgroup            *MergeVgroupStmt
		sMultiCreateTable       *MultiCreateTableStmt
		sRedistributeVgroup     *RedistributeVgroupStmt
		sResetQueryCache        *ResetQueryCacheStmt
		sRestoreComponentNode   *RestoreComponentNodeStmt
		sRestoreDnode           *RestoreDnodeStmt
		sRevokeRole             *RevokeRoleStmt
		sRollup                 *RollupStmt
		sScan                   *ScanStmt
		sSelect                 *SelectStmt
		sShow                   *ShowStmt
		sSplitVgroup            *SplitVgroupStmt
		sSsMigrateDatabase      *SsMigrateDatabaseStmt
		sStream                 *StreamStmt
		sTopic                  *TopicStmt
		sTrimDatabase           *TrimDatabaseStmt
		sTrimDatabaseWal        *TrimDatabaseWalStmt
		sUpdateAnode            *UpdateAnodeStmt
		sUseDatabase            *UseDatabaseStmt
		sXnode                  *XnodeStmt
	)

	statements := []Statement{
		sAlterCluster,
		sAlterDatabase,
		sAlterDnode,
		sAlterDnodesReload,
		sAlterEncryptKey,
		sAlterLocal,
		sAlterNamed,
		sAlterRole,
		sAlterTable,
		sAlterToken,
		sAlterUser,
		sAlterVgroupKeep,
		sAssignLeader,
		sBalanceVgroupLeader,
		sBalanceVgroup,
		sCompact,
		sCreateAnode,
		sCreateBnode,
		sCreateComponentNode,
		sCreateDatabase,
		sCreateDnode,
		sCreateEncryptAlgr,
		sCreateMount,
		sCreateNamed,
		sCreateRole,
		sCreateSubTableFromFile,
		sCreateTable,
		sCreateToken,
		sCreateUser,
		sCreateView,
		sCreateVSubTable,
		sDelete,
		sDescribe,
		sDropAnode,
		sDropBnode,
		sDropComponentNode,
		sDropDatabase,
		sDropDnode,
		sDropEncryptAlgr,
		sCreateFunction,
		sDropFunction,
		sDropMount,
		sDropNamed,
		sDropRole,
		sDropTable,
		sDropToken,
		sDropUser,
		sDropView,
		sExplain,
		sFlushDatabase,
		sGrantRole,
		sGrant,
		sInsertQuery,
		sKill,
		sMergeVgroup,
		sMultiCreateTable,
		sRedistributeVgroup,
		sResetQueryCache,
		sRestoreComponentNode,
		sRestoreDnode,
		sRevokeRole,
		sRollup,
		sScan,
		sSelect,
		sShow,
		sSplitVgroup,
		sSsMigrateDatabase,
		sStream,
		sTopic,
		sTrimDatabase,
		sTrimDatabaseWal,
		sUpdateAnode,
		sUseDatabase,
		sXnode,
	}

	for _, stmt := range statements {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Format panic on nil statement %T: %v", stmt, r)
				}
			}()
			tb := newTB()
			stmt.Format(tb)
		}()
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Walk panic on nil statement %T: %v", stmt, r)
				}
			}()
			if err := Walk(func(node SQLNode) (bool, error) { return true, nil }, stmt); err != nil {
				t.Fatalf("Walk failed on nil statement %T: %v", stmt, err)
			}
		}()
	}
}
