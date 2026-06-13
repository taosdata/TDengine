package sqlparser

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"sqlparser/tool"
)

type testLexer struct{ errs []string }

func (l *testLexer) Lex(*yySymType) int { return 0 }
func (l *testLexer) Error(s string)     { l.errs = append(l.errs, s) }

func TestShowStmt_FormatBranchesAndConstructors(t *testing.T) {
	cases := []struct {
		n string
		s *ShowStmt
		w string
	}{
		{"db_kind", &ShowStmt{Kind: "databases", DBKind: "user"}, "show user databases"},
		{"dnode_like", &ShowStmt{Kind: "dnode_variables", ID: 7, Pattern: "x%"}, "show dnode 7 variables like 'x%'"},
		{"txn", &ShowStmt{Kind: "transaction", ID: 1}, "show transaction 1"},
		{"vnodes_id", &ShowStmt{Kind: "vnodes", HasID: true, ID: 9}, "show vnodes on dnode 9"},
		{"streams_db_like", &ShowStmt{Kind: "streams", DBName: "db1", Pattern: "s%"}, "show db1. streams like 's%'"},
		{"indexes", &ShowStmt{Kind: "indexes", DBName: "db1", Table: "t1"}, "show indexes from db1.t1"},
		{"table_tags", &ShowStmt{Kind: "table_tags", DBName: "db1", Table: "t1", TagItems: []string{"tbname", "tag1"}}, "show table tags tbname,tag1 from db1.t1"},
		{"table_distributed", &ShowStmt{Kind: "table_distributed", Object: "db1.t1"}, "show table distributed db1.t1"},
		{"show_create_rsma", &ShowStmt{Kind: "show_create_rsma", Object: "db1.r1"}, "show create rsma db1.r1"},
		{"xnode", &ShowStmt{Kind: "xnode", Object: "xn1"}, "show xnode xn1"},
		{"tables_scope_like", &ShowStmt{Kind: "tables", TableKind: "child", DBName: "db1", Pattern: "t_%"}, "show child db1. tables like 't_%'"},
		{"variables_like", &ShowStmt{Kind: "variables", Pattern: "v%"}, "show variables like 'v%'"},
		{"local_variables_like", &ShowStmt{Kind: "local_variables", Pattern: "v%"}, "show local variables like 'v%'"},
		{"instances_like", &ShowStmt{Kind: "instances", Pattern: "i%"}, "show instances like 'i%'"},
		{"default", &ShowStmt{Kind: "anything"}, "show anything"},
	}
	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			tb := newTB()
			tc.s.Format(tb)
			if got := strings.TrimSpace(tb.String()); got != tc.w {
				t.Fatalf("unexpected show format: got=%q want=%q", got, tc.w)
			}
		})
	}

	if s := NewShowDatabasesStmt("system"); s.Kind != "databases" || s.DBKind != "system" {
		t.Fatalf("unexpected NewShowDatabasesStmt: %+v", s)
	}
	if s := NewShowStmtWithPattern("variables", "x%"); s.Pattern != "x%" {
		t.Fatalf("unexpected NewShowStmtWithPattern: %+v", s)
	}
	if s := NewShowStmtWithDB("streams", "db1"); s.DBName != "db1" {
		t.Fatalf("unexpected NewShowStmtWithDB: %+v", s)
	}
	if s := NewShowStmtWithDBPattern("streams", "db1", "s%"); s.DBName != "db1" || s.Pattern != "s%" {
		t.Fatalf("unexpected NewShowStmtWithDBPattern: %+v", s)
	}
	if s := NewShowStmtWithTableDB("indexes", "t1", "db1"); s.Table != "t1" || s.DBName != "db1" {
		t.Fatalf("unexpected NewShowStmtWithTableDB: %+v", s)
	}
	if s := NewShowStmtWithTableScope("tables", ShowTableScope{TableKind: "child", DBName: "db1"}, "t_%"); s.TableKind != "child" || s.DBName != "db1" || s.Pattern != "t_%" {
		t.Fatalf("unexpected NewShowStmtWithTableScope: %+v", s)
	}
	if s := NewShowStmtWithTableDBTags("table_tags", "t1", "db1", []string{"tb"}); s.Table != "t1" || s.DBName != "db1" || len(s.TagItems) != 1 {
		t.Fatalf("unexpected NewShowStmtWithTableDBTags: %+v", s)
	}
	if s := NewShowStmtWithObject("xnode", "x1"); s.Object != "x1" {
		t.Fatalf("unexpected NewShowStmtWithObject: %+v", s)
	}
	if s := NewShowStmtWithID("vnodes", 2); !s.HasID || s.ID != 2 {
		t.Fatalf("unexpected NewShowStmtWithID: %+v", s)
	}
	if s := NewShowStmtWithIDPattern("dnode_variables", 3, "k%"); !s.HasID || s.ID != 3 || s.Pattern != "k%" {
		t.Fatalf("unexpected NewShowStmtWithIDPattern: %+v", s)
	}
}

func TestExprTableName_EdgeBranches(t *testing.T) {
	tb := newTB()
	NewTableIdent("select").Format(tb)
	if got := tb.String(); got != "select" {
		t.Fatalf("unexpected format for identifier, got %q", got)
	}

	tb = newTB()
	NewTableIdent("1abc").Format(tb)
	if got := tb.String(); got != "`1abc`" {
		t.Fatalf("expected leading digit escaped, got %q", got)
	}

	tb = newTB()
	NewTableIdent("@@sys.var").Format(tb)
	if got := tb.String(); got != "`@@sys.var`" {
		t.Fatalf("expected system var escaped by current formatter, got %q", got)
	}

	tb = newTB()
	NewTableIdent("a`b").Format(tb)
	if got := tb.String(); got != "`a``b`" {
		t.Fatalf("expected backtick doubled, got %q", got)
	}

	if got := compliantName("-A.1"); got != "_A_1" {
		t.Fatalf("unexpected compliantName: %q", got)
	}

	n := TableName{Qualifier: NewTableIdent("DBX"), Name: NewTableIdent("MiXeD")}
	vn := n.ToViewName()
	if vn.Qualifier.String() != "DBX" || vn.Name.String() != "mixed" {
		t.Fatalf("unexpected ToViewName: %+v", vn)
	}

	if err := n.walkSubtree(func(node SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("tablename walk failed: %v", err)
	}
}

func TestDatabaseHelpers_Branches(t *testing.T) {
	if v, ok := normalizeByteList([][]byte{[]byte("1")}); !ok || len(v) != 1 {
		t.Fatalf("normalizeByteList [][]byte failed: %v %v", v, ok)
	}
	if v, ok := normalizeByteList([]byte("2")); !ok || len(v) != 1 {
		t.Fatalf("normalizeByteList []byte failed: %v %v", v, ok)
	}
	if _, ok := normalizeByteList(123); ok {
		t.Fatalf("normalizeByteList should reject invalid type")
	}

	if got, err := parseCompactRangeValue([]byte("2h")); err != nil || got != 120 {
		t.Fatalf("parseCompactRangeValue duration failed: got=%d err=%v", got, err)
	}
	if got, err := parseCompactRangeValue([]byte("2n")); err != nil || got != 2*30*24*60 {
		t.Fatalf("parseCompactRangeValue month failed: got=%d err=%v", got, err)
	}
	if got, err := parseCompactRangeValue([]byte("15")); err != nil || got != 15 {
		t.Fatalf("parseCompactRangeValue int failed: got=%d err=%v", got, err)
	}
	if _, err := parseCompactRangeValue([]byte("xx")); err == nil {
		t.Fatalf("parseCompactRangeValue should fail for invalid int")
	}

	if got := tdDurationToSQLLiteral(tool.NewTDDurationWithMonth(24)); got != "2y" {
		t.Fatalf("unexpected duration to sql literal year: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDurationWithMonth(5)); got != "5n" {
		t.Fatalf("unexpected duration to sql literal month: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDuration(24 * time.Hour)); got != "1d" {
		t.Fatalf("unexpected duration to sql literal day: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDuration(2 * time.Hour)); got != "2h" {
		t.Fatalf("unexpected duration to sql literal hour: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDuration(3 * time.Minute)); got != "3m" {
		t.Fatalf("unexpected duration to sql literal minute: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDuration(4 * time.Second)); got != "4s" {
		t.Fatalf("unexpected duration to sql literal sec: %q", got)
	}
	if got := tdDurationToSQLLiteral(tool.NewTDDuration(1500 * time.Millisecond)); got != "1500a" {
		t.Fatalf("unexpected duration to sql literal ms: %q", got)
	}

	if got := quoteIdentifierIfNeeded("db1"); got != "db1" {
		t.Fatalf("unexpected quoteIdentifierIfNeeded lower: %q", got)
	}
	if got := quoteIdentifierIfNeeded("Db1"); got != "`Db1`" {
		t.Fatalf("unexpected quoteIdentifierIfNeeded upper: %q", got)
	}
	if got := quoteIdentifierIfNeeded("db-1"); got != "`db-1`" {
		t.Fatalf("unexpected quoteIdentifierIfNeeded punct: %q", got)
	}

	opts := &DatabaseOptions{
		Buffer:                  1,
		CacheModelStr:           "none",
		CacheLastSize:           2,
		CompressionLevel:        1,
		DaysPerFile:             []tool.TDDuration{tool.NewTDDuration(24 * time.Hour)},
		MaxRowsPerBlock:         3,
		MinRowsPerBlock:         4,
		Pages:                   5,
		Pagesize:                6,
		TsdbPageSize:            7,
		PrecisionStr:            "ms",
		Replica:                 2,
		WalLevel:                1,
		fsyncPeriod:             8,
		WalRetentionPeriod:      9,
		WalRetentionPeriodIsSet: true,
		WalRetentionSize:        10,
		WalRetentionSizeIsSet:   true,
		WalRollPeriod:           11,
		WalRollPeriodIsSet:      true,
		WalSegmentSize:          12,
		WalSegmentSizeIsSet:     true,
		SstTrigger:              13,
		TablePrefix:             14,
		TableSuffix:             15,
		SsChunkSize:             16,
		SsKeepLocal:             17,
		SsCompact:               1,
		KeepTimeOffset:          18,
		Keep:                    []tool.TDDuration{tool.NewTDDuration(48 * time.Hour)},
		DnodeListStr:            "1,2",
		EncryptAlgorithmStr:     "aes",
		CompactInterval:         19,
		CompactStartTime:        20,
		CompactEndTime:          21,
		CompactTimeOffset:       22,
		IsAudit:                 1,
		Retentions:              []tool.TDDuration{tool.NewTDDuration(24 * time.Hour), tool.NewTDDuration(48 * time.Hour)},
	}
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}
	appendDatabaseOptions(tb, opts)
	out := tb.String()
	for _, must := range []string{"buffer 1", "cachemodel 'none'", "retentions 1d:2d", "compact_time_range 20,21", "encrypt_algorithm 'aes'"} {
		if !strings.Contains(out, must) {
			t.Fatalf("appendDatabaseOptions missing %q in %q", must, out)
		}
	}

	// Hit SetDatabaseOption success and error branches.
	lx := &testLexer{}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_REPLICA, []byte("2")); got == nil || !got.WithArbitrator {
		t.Fatalf("expected replica option to set arbitrator")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_DAYS, [][]byte{[]byte("2d"), []byte("3")}); got == nil || len(got.DaysPerFile) != 2 {
		t.Fatalf("expected days option parsed: %+v", got)
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_KEEP, [][]byte{[]byte("1d"), []byte("2")}); got == nil || len(got.Keep) != 2 {
		t.Fatalf("expected keep option parsed: %+v", got)
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_RETENTIONS, [][]byte{[]byte("1d:2d")}); got == nil || len(got.Retentions) != 2 {
		t.Fatalf("expected retentions parsed: %+v", got)
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_COMPACT_TIME_RANGE, [][]byte{[]byte("2h"), []byte("30")}); got == nil || got.CompactStartTime != 120 || got.CompactEndTime != 30 {
		t.Fatalf("expected compact time range parsed: %+v", got)
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_BUFFER, []byte("x")); got != nil {
		t.Fatalf("expected invalid buffer to return nil")
	}
	if len(lx.errs) == 0 {
		t.Fatalf("expected lexer errors on invalid option")
	}
}

func TestLowCoverage_DefensiveBranches(t *testing.T) {
	lx := &testLexer{}

	// SetTableOption nil-options and parse-error branches not reachable from grammar reductions.
	if got := SetTableOption(lx, nil, TABLE_OPTION_COMMENT, []byte("c")); got == nil || got.Comment != "c" {
		t.Fatalf("expected nil options to be initialized for table comment, got %+v", got)
	}
	if got := SetTableOption(lx, nil, TABLE_OPTION_MAXDELAY, []byte("1hh")); got != nil {
		t.Fatalf("expected invalid maxdelay []byte to fail")
	}
	if got := SetTableOption(lx, nil, TABLE_OPTION_KEEP, [][]byte{[]byte("1hh")}); got != nil {
		t.Fatalf("expected invalid keep [][]byte to fail")
	}

	// SetDatabaseOption defensive type checks.
	if got := SetDatabaseOption(lx, nil, DB_OPTION_DAYS, 1); got != nil {
		t.Fatalf("expected invalid days type to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_KEEP, 1); got != nil {
		t.Fatalf("expected invalid keep type to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_COMPACT_TIME_RANGE, 1); got != nil {
		t.Fatalf("expected invalid compact_time_range type to fail")
	}

	// SetDatabaseOption duration/int parse error branches.
	if got := SetDatabaseOption(lx, nil, DB_OPTION_DAYS, [][]byte{[]byte("1hh")}); got != nil {
		t.Fatalf("expected invalid days duration to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_DAYS, [][]byte{[]byte("1abc")}); got != nil {
		t.Fatalf("expected invalid days int-like token to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_KEEP, [][]byte{[]byte("1hh")}); got != nil {
		t.Fatalf("expected invalid keep duration to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_KEEP, [][]byte{[]byte("1abc")}); got != nil {
		t.Fatalf("expected invalid keep int-like token to fail")
	}

	// STRICT branch currently lacks SQL path in grammar, cover via direct setter.
	if got := SetDatabaseOption(lx, nil, DB_OPTION_STRICT, []byte("on")); got == nil || got.StrictStr != "on" {
		t.Fatalf("expected strict option to be assigned, got %+v", got)
	}

	// Retentions branches: skip "-", parse duration error, parse int error, and int success append.
	if got := SetDatabaseOption(lx, nil, DB_OPTION_RETENTIONS, [][]byte{[]byte("-:2")}); got == nil || len(got.Retentions) != 1 {
		t.Fatalf("expected retentions '-:2' to keep one duration, got %+v", got)
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_RETENTIONS, [][]byte{[]byte("1hh:2d")}); got != nil {
		t.Fatalf("expected invalid retention duration to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_RETENTIONS, [][]byte{[]byte("1d:a1")}); got != nil {
		t.Fatalf("expected invalid retention integer token to fail")
	}
	if got := SetDatabaseOption(lx, nil, DB_OPTION_RETENTIONS, [][]byte{[]byte("1d:2")}); got == nil || len(got.Retentions) != 2 {
		t.Fatalf("expected retention integer append to succeed, got %+v", got)
	}

	// Alter option helpers nil branches.
	if out := AddAlterDatabaseOption(nil, DatabaseOptionKV{Type: DB_OPTION_BUFFER, Value: []byte("1")}); out == nil || len(out.Items) != 1 {
		t.Fatalf("expected AddAlterDatabaseOption(nil, ...) to create list, got %+v", out)
	}
	if out := ApplyAlterDatabaseOptions(lx, nil); out != nil {
		t.Fatalf("expected ApplyAlterDatabaseOptions(nil) to return nil, got %+v", out)
	}

	// parseCompactRangeValue ParseDuration-error path.
	if _, err := parseCompactRangeValue([]byte("1hh")); err == nil {
		t.Fatalf("expected parseCompactRangeValue duration parse error")
	}

	// cloneUserOptions nil and DropTimeRanges-copy branch.
	if cloneUserOptions(nil) != nil {
		t.Fatalf("expected cloneUserOptions(nil) == nil")
	}
	src := &UserOptions{DropTimeRanges: []*DateTimeRange{{Duration: 1}}}
	cl := cloneUserOptions(src)
	if cl == nil || len(cl.DropTimeRanges) != 1 {
		t.Fatalf("expected cloned drop time ranges, got %+v", cl)
	}

	// Insert/topic formatting defensive branches.
	if got := formatInsertIdentifier([]byte("")); got != "" {
		t.Fatalf("expected empty insert identifier unchanged, got %q", got)
	}
	tb := newTB()
	(&TopicStmt{Name: "tp1", MetaMode: "meta_custom", Database: "db1"}).Format(tb)
	if got := strings.TrimSpace(tb.String()); !strings.Contains(got, "meta_custom") {
		t.Fatalf("expected topic default meta branch in format, got %q", got)
	}

	// Marker interface methods.
	(&CreateDatabaseStmt{}).iStatement()
	(&DropDatabaseStmt{}).iStatement()
	(&UseDatabaseStmt{}).iStatement()
	(&FlushDatabaseStmt{}).iStatement()
	(&SsMigrateDatabaseStmt{}).iStatement()
	(&TrimDatabaseStmt{}).iStatement()
	(&TrimDatabaseWalStmt{}).iStatement()
	(&CreateTableStmt{}).iStatement()
	var insStmt InsertStatement
	insStmt.iStatement()
	(&CreateUserStmt{}).iStatement()
	(&AlterUserStmt{}).iStatement()
	(&DropUserStmt{}).iStatement()
	(&TopicStmt{}).iStatement()
}
