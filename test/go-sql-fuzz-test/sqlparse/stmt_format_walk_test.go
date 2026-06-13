package sqlparser

import (
	"strings"
	"testing"
)

func TestStatementFormat_NoLongerEmpty(t *testing.T) {
	cases := []struct {
		name     string
		node     SQLNode
		contains string
	}{
		{name: "create user", node: &CreateUserStmt{UserName: "u1"}, contains: "create user"},
		{name: "alter user", node: &AlterUserStmt{UserName: "u1"}, contains: "alter user"},
		{name: "drop user", node: &DropUserStmt{UserName: "u1"}, contains: "drop user"},
		{name: "create token", node: &CreateTokenStmt{Name: "t1", User: "u1"}, contains: "create token"},
		{name: "drop token", node: &DropTokenStmt{Name: "t1"}, contains: "drop token"},
		{name: "create role", node: &CreateRoleStmt{Name: "r1"}, contains: "create role"},
		{name: "grant role", node: &GrantRoleStmt{RoleName: "r1", GranteeName: "u1"}, contains: "grant role"},
		{name: "create db", node: &CreateDatabaseStmt{DbName: "db1"}, contains: "create database"},
		{name: "create db options", node: &CreateDatabaseStmt{DbName: "db1", Options: &DatabaseOptions{Buffer: 10, WalRetentionPeriod: -1, WalRetentionPeriodIsSet: true}}, contains: "wal_retention_period -1"},
		{name: "drop db", node: &DropDatabaseStmt{DbName: "db1"}, contains: "drop database"},
		{name: "use db", node: &UseDatabaseStmt{DbName: "db1"}, contains: "use db1"},
		{name: "create dnode", node: &CreateDnodeStmt{Fqdn: "127.0.0.1"}, contains: "create dnode"},
		{name: "drop anode", node: &DropAnodeStmt{AnodeId: 1}, contains: "drop anode"},
		{name: "create table", node: &CreateTableStmt{TableName: &TableName{Name: NewTableIdent("t1")}}, contains: "create table"},
		{name: "create table cols tags", node: &CreateTableStmt{TableName: &TableName{Name: NewTableIdent("t1")}, Columns: []*ColumnDef{{ColName: "ts"}, {ColName: "v"}}, Tags: []*ColumnDef{{ColName: "tg1"}}}, contains: "tags (tg1 int)"},
		{name: "create table options", node: &CreateTableStmt{TableName: &TableName{Name: NewTableIdent("t2")}, Options: &TableOptions{Comment: "x", TTL: 10}}, contains: "ttl 10"},
		{name: "create view", node: &CreateViewStmt{Name: "v1"}, contains: "create view"},
		{name: "describe", node: &DescribeStmt{Table: "t1"}, contains: "describe"},
		{name: "explain", node: &ExplainStmt{}, contains: "explain"},
		{name: "insert query", node: &InsertQueryStmt{Table: "t1"}, contains: "insert into"},
		{name: "reset query cache", node: &ResetQueryCacheStmt{}, contains: "reset query cache"},
		{name: "topic", node: &TopicStmt{Name: "tp"}, contains: "create topic"},
		{name: "stream recalculate range", node: &StreamStmt{Action: "recalculate", Names: []string{"s1"}, RecalcFrom: "1", RecalcTo: "2"}, contains: "from 1 to 2"},
		{name: "stream create", node: &StreamStmt{Action: "create", NotExists: true, Names: []string{"s1"}, Trigger: "session(ts, 10s)", OutTable: "into db1.tout (c1)", Query: &SelectStmt{}}, contains: "create stream if not exists s1"},
		{name: "show user databases", node: &ShowStmt{Kind: "databases", DBKind: "user"}, contains: "show user databases"},
		{name: "show streams with db", node: &ShowStmt{Kind: "streams", DBName: "db1"}, contains: "db1. streams"},
		{name: "show indexes with table db", node: &ShowStmt{Kind: "indexes", Table: "t1", DBName: "db1"}, contains: "from db1.t1"},
		{name: "show tables scope", node: &ShowStmt{Kind: "tables", TableKind: "child", DBName: "db1", Pattern: "t_%"}, contains: "child"},
		{name: "show table tags list", node: &ShowStmt{Kind: "table_tags", Table: "t1", DBName: "db1", TagItems: []string{"tbname", "tag1"}}, contains: "tags tbname,tag1"},
		{name: "show create object", node: &ShowStmt{Kind: "show_create_table", Object: "db1.t1"}, contains: "db1.t1"},
		{name: "show transaction id", node: &ShowStmt{Kind: "transaction", ID: 7, HasID: true}, contains: "transaction 7"},
		{name: "show tables like", node: &ShowStmt{Kind: "tables", Pattern: "t_%"}, contains: "like 't_%'"},
		{name: "topic drop group", node: &TopicStmt{DropGroup: true, GroupName: "g1", OnTopic: "tp1"}, contains: "drop consumer group"},
		{name: "xnode user pass", node: &XnodeStmt{Action: "create", User: "u1", Pass: "p1"}, contains: "user u1 pass 'p1'"},
		{name: "xnode task fields", node: &XnodeStmt{Action: "alter", ResourceType: "task", ID: 1, TaskFrom: "src", TaskTo: "db1", TaskOptions: "parser = x"}, contains: "with parser = x"},
		{name: "alter rsma funcs", node: &AlterNamedStmt{Kind: "rsma", Name: "db1.r1", IfExists: true, Funcs: []string{"a", "b"}}, contains: "function(a, b)"},
		{name: "multi create table", node: &MultiCreateTableStmt{Entries: []MultiCreateTableEntry{{Target: "db1.t1", Using: "db1.st1"}}}, contains: "using"},
		{name: "create sub table from file tags", node: &CreateSubTableFromFileStmt{NotExists: true, Using: "db1.st1", TagItems: []string{"tbname", "qtags"}, File: "f1"}, contains: "(tbname, qtags)"},
		{name: "create vsub with refs", node: &CreateVSubTableStmt{NotExists: true, Target: "db1.vt1", Using: "db1.st1", SpecificCols: []string{"tbname"}, RefCols: []string{"c1 from s1"}}, contains: "refs (c1 from s1)"},
		{name: "multi create table options", node: &MultiCreateTableStmt{Entries: []MultiCreateTableEntry{{Target: "db1.t1", Using: "db1.st1", Options: &TableOptions{TTL: 10, Comment: "x"}}}}, contains: "comment 'x' ttl 10"},
		{name: "delete where", node: &DeleteStmt{Table: "t1", Where: &RawExpr{Kind: "col", Name: "c1"}}, contains: "where"},
		{name: "create view as", node: &CreateViewStmt{Name: "v1", Query: &SelectStmt{}}, contains: "as select"},
		{name: "explain options", node: &ExplainStmt{Options: ExplainOptions{VerboseSet: true, Verbose: true}}, contains: "verbose true"},
		{name: "scan start end", node: &ScanStmt{Scope: "database", Name: "db1", Start: "1", End: "2"}, contains: "start with 1"},
		{name: "compact start end", node: &CompactStmt{Scope: "database", Name: "db1", Start: "1", End: "2"}, contains: "end with 2"},
		{name: "rollup start end", node: &RollupStmt{Scope: "database", Name: "db1", Start: "1", End: "2"}, contains: "start with 1"},
		{name: "alter database options", node: &AlterDatabaseStmt{Name: "db1", Options: &DatabaseOptions{Buffer: 10, WalLevel: 1}}, contains: "wal_level 1"},
		{name: "alter database precision replica", node: &AlterDatabaseStmt{Name: "db1", Options: &DatabaseOptions{Replica: 2, PrecisionStr: "ms"}}, contains: "precision 'ms'"},
		{name: "grant write", node: &GrantStmt{OptrType: 0, Principal: "u1", Privileges: PrivSetArgs{PrivArgs: PRIV_TYPE_UNKNOWN}, PrivilegeName: "write"}, contains: "grant write to u1"},
		{name: "grant with cond", node: &GrantStmt{OptrType: 0, Principal: "u1", Privileges: PrivSetArgs{PrivArgs: PRIV_CM_SHOW}, PrivilegeName: "show", Cond: &RawExpr{Kind: "cmp", Left: &RawExpr{Kind: "col", Name: "v"}, Op: Token{Bytes: []byte("gt")}, Right: Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}}}, contains: "with v > 1"},
		{name: "select", node: &SelectStmt{IsDistinct: true}, contains: "select distinct"},
		{name: "raw expr", node: &RawExpr{Kind: "binary", Name: "c1"}, contains: "c1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTB()
			tc.node.Format(tb)
			got := tb.String()
			if !strings.Contains(got, tc.contains) {
				t.Fatalf("unexpected format output: got=%q want contain=%q", got, tc.contains)
			}
		})
	}
}

func TestStatementWalk_SubtreeReachable(t *testing.T) {
	selectNode := &SelectStmt{
		Select: []Expr{&RawExpr{Kind: "col", Name: "c1"}},
		From:   &TableNameExpr{DBName: "db1", TableName: "t1"},
		Where:  &RawExpr{Kind: "cmp", Left: &RawExpr{Kind: "col", Name: "c1"}, Right: Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}},
	}

	view := &CreateViewStmt{Name: "v1", Query: selectNode}
	explain := &ExplainStmt{Target: selectNode}
	insertQuery := &InsertQueryStmt{Table: "t1", Query: selectNode}
	topic := &TopicStmt{Name: "tp", Query: selectNode, Where: &RawExpr{Kind: "col", Name: "w1"}}
	insertStmt := InsertStatement{
		{
			TableName: &TableName{Name: NewTableIdent("t")},
			Using: &UsingClause{
				TableName: &TableName{Name: NewTableIdent("st")},
			},
			Values: [][]*SQLVal{{NewIntVal([]byte("1"))}},
		},
	}

	grant := &GrantStmt{OptrType: 0, Principal: "u1", PrivilegeName: "show", Cond: &RawExpr{Kind: "cmp", Left: &RawExpr{Kind: "col", Name: "v"}, Op: Token{Bytes: []byte("gt")}, Right: Literal{Val: Token{Bytes: []byte("1")}, Type: LiteralInt}}}
	stream := &StreamStmt{Action: "create", Names: []string{"s1"}, Query: selectNode}
	nodes := []SQLNode{view, explain, insertQuery, topic, insertStmt, grant, stream}
	for _, n := range nodes {
		visited := 0
		if err := Walk(func(node SQLNode) (bool, error) {
			visited++
			return true, nil
		}, n); err != nil {
			t.Fatalf("walk failed for %T: %v", n, err)
		}
		if visited < 2 {
			t.Fatalf("expected subtree nodes for %T, visited=%d", n, visited)
		}
	}
}

func TestExplainWalk_IncludesRatio(t *testing.T) {
	explain := &ExplainStmt{
		Options: ExplainOptions{
			RatioSet: true,
			Ratio:    Literal{Val: Token{Bytes: []byte("0.5")}, Type: LiteralFloat},
		},
	}
	visitedRatio := false
	if err := Walk(func(node SQLNode) (bool, error) {
		if lit, ok := node.(Literal); ok && lit.Type == LiteralFloat {
			visitedRatio = true
		}
		return true, nil
	}, explain); err != nil {
		t.Fatalf("walk failed: %v", err)
	}
	if !visitedRatio {
		t.Fatalf("expected explain walk to visit ratio literal")
	}
}
