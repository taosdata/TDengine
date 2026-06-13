package sqlparser

import (
	"errors"
	"sqlparser/tool"
	"strings"
	"testing"
	"time"
)

type errCollectLexer struct{ errs []string }

func (l *errCollectLexer) Lex(*yySymType) int { return 0 }
func (l *errCollectLexer) Error(s string)     { l.errs = append(l.errs, s) }

func invokeStatementMarker(s interface{ iStatement() }) { s.iStatement() }

func tokBytes(s string) Token { return Token{Bytes: []byte(s)} }

func TestStmtCoverage_UserHelpersAndSetters(t *testing.T) {
	lx := &errCollectLexer{}

	if _, err := NewIpRange([]byte("127.0.0.1/32"), 0); err != nil {
		t.Fatalf("NewIpRange valid failed: %v", err)
	}
	if got := AppendIpRange(lx, nil, tokBytes("bad-cidr")); len(got) != 0 {
		t.Fatalf("AppendIpRange should keep empty on error: %+v", got)
	}

	if _, err := parseDateTimeRange([]byte("")); err == nil {
		t.Fatalf("parseDateTimeRange should fail for empty")
	}
	if got, err := parseDateTimeRange([]byte("-2h")); err != nil || got.Duration != 2*3600 || got.Neg != 1 {
		t.Fatalf("parseDateTimeRange duration failed: got=%+v err=%v", got, err)
	}
	if got, err := parseDateTimeRange([]byte("2")); err != nil || got.Duration != 2*24*3600 {
		t.Fatalf("parseDateTimeRange days failed: got=%+v err=%v", got, err)
	}
	if got, err := parseDateTimeRange([]byte("2024-01-02")); err != nil || got.Year != 2024 || got.Month != 1 || got.Day != 2 {
		t.Fatalf("parseDateTimeRange date failed: got=%+v err=%v", got, err)
	}
	if got, err := parseDateTimeRange([]byte("2024-01-02 03:04")); err != nil || got.Hour != 3 || got.Minute != 4 {
		t.Fatalf("parseDateTimeRange datetime failed: got=%+v err=%v", got, err)
	}
	if got := AppendDateTimeRange(lx, nil, tokBytes("x")); got != nil {
		t.Fatalf("AppendDateTimeRange should return nil on parse error")
	}

	if got := formatUserOptionValue(-1, 1, true); got != "unlimited" {
		t.Fatalf("unexpected formatUserOptionValue unlimited: %q", got)
	}
	if got := formatUserOptionValue(120, 60, false); got != "2" {
		t.Fatalf("unexpected formatUserOptionValue divide: %q", got)
	}
	if got := formatDateTimeRangeLiteral(nil); got != "" {
		t.Fatalf("unexpected nil datetime literal: %q", got)
	}
	if got := formatDateTimeRangeLiteral(&DateTimeRange{Neg: 1, Duration: 3600}); got != "-3600s" {
		t.Fatalf("unexpected duration datetime literal: %q", got)
	}
	if got := formatDateTimeRangeLiteral(&DateTimeRange{Year: 2024, Month: 1, Day: 2}); got != "2024-01-02" {
		t.Fatalf("unexpected date datetime literal: %q", got)
	}
	if got := formatDateTimeRangeLiteral(&DateTimeRange{Year: 2024, Month: 1, Day: 2, Hour: 3, Minute: 4}); got != "2024-01-02 03:04" {
		t.Fatalf("unexpected datetime literal: %q", got)
	}

	buf := newTB()
	formatIPRangeList(buf, []*IpRange{{}, nil})
	if got := buf.String(); !strings.Contains(got, "''") {
		t.Fatalf("expected empty ip literal branch, got %q", got)
	}
	buf = newTB()
	formatDateTimeRangeList(buf, []*DateTimeRange{{Duration: 10}, nil})
	if got := buf.String(); !strings.Contains(got, "'10s'") || !strings.Contains(got, "''") {
		t.Fatalf("unexpected datetime range list: %q", got)
	}

	u := CreateDefaultUserOptions()
	u.setCallPerSession(lx, Token{Type: DEFAULT})
	u.setCallPerSession(lx, Token{Type: UNLIMITED})
	u.setCallPerSession(lx, tokBytes("3"))
	u.setCallPerSession(lx, tokBytes("bad"))

	u.setVnodePerCall(lx, Token{Type: DEFAULT})
	u.setVnodePerCall(lx, Token{Type: UNLIMITED})
	u.setVnodePerCall(lx, tokBytes("4"))
	u.setVnodePerCall(lx, tokBytes("bad"))

	u.setFailedLoginAttempts(lx, Token{Type: DEFAULT})
	u.setFailedLoginAttempts(lx, Token{Type: UNLIMITED})
	u.setFailedLoginAttempts(lx, tokBytes("5"))
	u.setFailedLoginAttempts(lx, tokBytes("bad"))

	u.setPasswordReuseMax(lx, Token{Type: DEFAULT})
	u.setPasswordReuseMax(lx, Token{Type: UNLIMITED})
	u.setPasswordReuseMax(lx, tokBytes("7"))
	u.setPasswordReuseMax(lx, tokBytes("bad"))

	host, _ := NewIpRange([]byte("10.0.0.0/24"), 0)
	timeR := &DateTimeRange{Year: 2024, Month: 1, Day: 2}
	u.setNotAllowHostList(lx, []*IpRange{host})
	u.setNotAllowDateTimeList(lx, []*DateTimeRange{timeR})
	u.setDropNotAllowHostList(lx, []*IpRange{host})
	u.setDropNotAllowDateTimeList(lx, []*DateTimeRange{timeR})
	if u.IpRanges[0].Neg != 1 || u.TimeRanges[0].Neg != 1 || u.DropIpRanges[0].Neg != 1 || u.DropTimeRanges[0].Neg != 1 {
		t.Fatalf("expected negation setters to set Neg=1")
	}

	u.HasPassword = true
	u.Password = "p"
	u.HasTotpseed = true
	u.Totpseed = ""
	u.HasEnable = true
	u.HasSysinfo = true
	u.HasIsImport = true
	u.HasCreatedb = true
	u.HasChangepass = true
	u.HasSessionPerUser = true
	u.HasConnectTime = true
	u.ConnectTime = -1
	u.HasConnectIdleTime = true
	u.ConnectIdleTime = -1
	u.HasAllowTokenNum = true

	au := NewAlterUserStmt(lx, "u1", u)
	au.iStatement()
	buf = newTB()
	au.Format(buf)
	if got := buf.String(); !strings.Contains(got, "add not_allow_host") || !strings.Contains(got, "drop not_allow_datetime") || !strings.Contains(got, "totpseed null") {
		t.Fatalf("unexpected alter user format: %q", got)
	}

	cu := NewCreateUserStmt(lx, "u1", nil, true)
	invokeStatementMarker(cu)
	buf = newTB()
	cu.Format(buf)
	if got := buf.String(); !strings.Contains(got, "create user if not exists u1") {
		t.Fatalf("unexpected create user format: %q", got)
	}

	optsA := &UserOptions{HasEnable: true, Enable: 0, IpRanges: []*IpRange{host}}
	optsB := &UserOptions{HasPassword: true, Password: "x", DropTimeRanges: []*DateTimeRange{{Duration: 1}}}
	merged := MergeUserOptions(lx, optsA, optsB)
	if !merged.HasPassword || merged.Password != "x" || len(merged.IpRanges) != 1 || len(merged.DropTimeRanges) != 1 {
		t.Fatalf("unexpected merged options: %+v", merged)
	}
	if got := MergeUserOptions(lx, nil, nil); got == nil {
		t.Fatalf("MergeUserOptions nil,nil should return defaults")
	}
	if got := MergeUserOptions(lx, nil, optsA); got == optsA {
		t.Fatalf("MergeUserOptions should clone when lhs nil")
	}
	if got := MergeUserOptions(lx, optsA, nil); got == optsA {
		t.Fatalf("MergeUserOptions should clone when rhs nil")
	}

	d := NewDropUserStmt(lx, "u1", true)
	invokeStatementMarker(d)
	db := newTB()
	d.Format(db)
	if got := db.String(); got != "drop user if exists u1" {
		t.Fatalf("unexpected drop user format: %q", got)
	}

	if len(lx.errs) == 0 {
		t.Fatalf("expected lexer errors for invalid setter inputs")
	}
}

func TestStmtCoverage_CreateTableHelpers(t *testing.T) {
	lx := &errCollectLexer{}

	if got := formatDataType(DataType{Type: TSDB_DATA_TYPE_DECIMAL, Precision: 10, Scale: 2}); got != "decimal(10,2)" {
		t.Fatalf("unexpected decimal format: %q", got)
	}
	if got := formatDataType(DataType{Type: TSDB_DATA_TYPE_NCHAR, Bytes: 8}); got != "nchar(8)" {
		t.Fatalf("unexpected nchar format: %q", got)
	}
	if got := formatDataType(DataType{Type: 0}); got != "int" {
		t.Fatalf("unexpected fallback format: %q", got)
	}

	s := &CreateTableStmt{
		TableName:    &TableName{Name: NewTableIdent("t1")},
		IgnoreExists: true,
		Columns:      []*ColumnDef{{ColName: "ts", DataType: DataType{Type: TSDB_DATA_TYPE_TIMESTAMP}}, {ColName: "v", DataType: DataType{Type: TSDB_DATA_TYPE_INT}}},
		Tags:         []*ColumnDef{{ColName: "tg", DataType: DataType{Type: TSDB_DATA_TYPE_BINARY, Bytes: 8}}},
		Options: &TableOptions{
			Comment:    "c",
			MaxDelay:   []tool.TDDuration{tool.NewTDDuration(10 * time.Second)},
			Watermark:  []tool.TDDuration{tool.NewTDDuration(24 * time.Hour), tool.NewTDDuration(48 * time.Hour)},
			DeleteMark: []tool.TDDuration{tool.NewTDDuration(72 * time.Hour)},
			TTL:        3,
			Keep:       []tool.TDDuration{tool.NewTDDuration(24 * time.Hour)},
			RollupFuncs: []string{
				"avg",
			},
			SMA:        []string{"v", "v2"},
			VirtualStb: true,
		},
		IsStable: true,
	}
	invokeStatementMarker(s)
	buf := newTB()
	s.Format(buf)
	if got := buf.String(); !strings.Contains(got, "create stable if not exists") || !strings.Contains(got, "max_delay 10s") || !strings.Contains(got, "watermark 1d,2d") || !strings.Contains(got, "delete_mark 3d") || !strings.Contains(got, "keep 1d") || !strings.Contains(got, "rollup(avg)") || !strings.Contains(got, "sma(v,v2)") || !strings.Contains(got, "virtual 1") {
		t.Fatalf("unexpected create table format: %q", got)
	}
	appendTableOptions(nil, s.Options)
	appendTableOptions(buf, nil)
	buf = newTB()
	formatColumnDefList(buf, []*ColumnDef{
		{ColName: "", DataType: DataType{Type: TSDB_DATA_TYPE_INT}, Options: &ColumnOption{PrimaryKey: true}},
		{ColName: "v2", DataType: DataType{Type: TSDB_DATA_TYPE_INT}, Options: &ColumnOption{Encode: "delta", HasRef: true, RefDB: "db1", RefTable: "t1", RefColumn: "v1"}},
	})
	if got := buf.String(); !strings.Contains(got, "primary key") || !strings.Contains(got, "from db1.t1.v1") {
		t.Fatalf("unexpected formatted column list: %q", got)
	}

	s2 := &CreateTableStmt{IsVTable: true}
	buf = newTB()
	s2.Format(buf)
	if got := buf.String(); got != "create vtable " {
		t.Fatalf("unexpected vtable format: %q", got)
	}
	if err := s.walkSubtree(func(SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("walkSubtree failed: %v", err)
	}
	var nilStmt *CreateTableStmt
	if err := nilStmt.walkSubtree(func(SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("nil walkSubtree failed: %v", err)
	}
	if err := (&CreateTableStmt{}).walkSubtree(func(SQLNode) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("walkSubtree with nil table name failed: %v", err)
	}

	opts := &TableOptions{}
	if SetTableOption(lx, opts, TABLE_OPTION_COMMENT, []byte("x")).Comment != "x" {
		t.Fatalf("table comment not set")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_MAXDELAY, []byte("1s")) == nil || len(opts.MaxDelay) == 0 {
		t.Fatalf("table maxdelay []byte failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_MAXDELAY, [][]byte{[]byte("2s")}) == nil || len(opts.MaxDelay) < 2 {
		t.Fatalf("table maxdelay [][]byte failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_MAXDELAY, 123) != nil {
		t.Fatalf("table maxdelay invalid type should fail")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_WATERMARK, [][]byte{[]byte("1s")}) == nil || len(opts.Watermark) != 1 {
		t.Fatalf("table watermark failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_ROLLUP, [][]byte{[]byte("avg")}) == nil || len(opts.RollupFuncs) == 0 {
		t.Fatalf("table rollup failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_TTL, []byte("9")) == nil || opts.TTL != 9 {
		t.Fatalf("table ttl failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_TTL, []byte("x")) != nil {
		t.Fatalf("table ttl invalid should fail")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_SMA, [][]byte{[]byte("x")}) == nil || len(opts.SMA) == 0 {
		t.Fatalf("table sma [][]byte failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_SMA, []string{"y"}) == nil {
		t.Fatalf("table sma []string failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_SMA, 123) != nil {
		t.Fatalf("table sma invalid should fail")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_DELETE_MARK, [][]byte{[]byte("1s")}) == nil || len(opts.DeleteMark) != 1 {
		t.Fatalf("table delete mark failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_KEEP, [][]byte{[]byte("1d")}) == nil || len(opts.Keep) == 0 {
		t.Fatalf("table keep [][]byte failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_KEEP, []byte("2d")) == nil || len(opts.Keep) != 1 {
		t.Fatalf("table keep duration bytes failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_KEEP, []byte("3")) == nil || len(opts.Keep) != 1 {
		t.Fatalf("table keep day-int bytes failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_KEEP, 123) == nil {
		t.Fatalf("table keep invalid type should still return options")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_VIRTUAL, []byte("1")) == nil || !opts.VirtualStb {
		t.Fatalf("table virtual=1 failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_VIRTUAL, []byte("0")) == nil || opts.VirtualStb {
		t.Fatalf("table virtual=0 failed")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_VIRTUAL, []byte("2")) != nil {
		t.Fatalf("table virtual invalid should fail")
	}
	if SetTableOption(lx, opts, 99, []byte("1")) != nil {
		t.Fatalf("unknown table option should fail")
	}

	_ = CreateDataType(TSDB_DATA_TYPE_INT)
	if dt := CreateVarLenDataType(lx, TSDB_DATA_TYPE_BINARY, []byte("0")); dt.Bytes <= 0 {
		t.Fatalf("expected default varlen bytes >0")
	}
	_ = CreateVarLenDataType(lx, TSDB_DATA_TYPE_NCHAR, []byte("3"))
	_ = CreateVarLenDataType(lx, TSDB_DATA_TYPE_BINARY, []byte("bad"))

	if dt := CreateDecimalDataType(lx, TSDB_DATA_TYPE_DECIMAL, []byte("10"), []byte("2")); dt.Precision != 10 || dt.Scale != 2 {
		t.Fatalf("unexpected decimal datatype: %+v", dt)
	}
	_ = CreateDecimalDataType(lx, TSDB_DATA_TYPE_DECIMAL, []byte("bad"), []byte("bad"))

	if got := decimalTypeFromPrecision(TSDB_DECIMAL64_MAX_PRECISION + 1); got != TSDB_DATA_TYPE_DECIMAL {
		t.Fatalf("unexpected decimal type for high precision: %d", got)
	}
	if got := decimalTypeFromPrecision(TSDB_DECIMAL64_MAX_PRECISION); got != TSDB_DATA_TYPE_DECIMAL64 {
		t.Fatalf("unexpected decimal64 type for in-range precision: %d", got)
	}

	if got := SetColumnOptionsPK(nil); !got.PrimaryKey {
		t.Fatalf("SetColumnOptionsPK failed")
	}
	if got := SetColumnOptions(lx, nil, []byte("encode"), []byte("rle")); got == nil || got.Encode != "rle" {
		t.Fatalf("SetColumnOptions encode failed")
	}
	if got := SetColumnOptions(lx, &ColumnOption{}, []byte("compress"), []byte("lz4")); got == nil || got.Compress != "lz4" {
		t.Fatalf("SetColumnOptions compress failed")
	}
	if got := SetColumnOptions(lx, &ColumnOption{}, []byte("level"), []byte("high")); got == nil || got.CompressLevel != "high" {
		t.Fatalf("SetColumnOptions level failed")
	}
	if got := SetColumnOptions(lx, &ColumnOption{}, []byte("bad"), []byte("x")); got != nil {
		t.Fatalf("SetColumnOptions unknown should fail")
	}

	if got := SetColumnReference(nil, "c1"); got.RefColumn != "c1" {
		t.Fatalf("SetColumnReference 1-part failed: %+v", got)
	}
	if got := SetColumnReference(nil, "t1.c1"); got.RefTable != "t1" || got.RefColumn != "c1" {
		t.Fatalf("SetColumnReference 2-part failed: %+v", got)
	}
	if got := SetColumnReference(nil, "db1.t1.c1"); got.RefDB != "db1" || got.RefTable != "t1" || got.RefColumn != "c1" {
		t.Fatalf("SetColumnReference 3-part failed: %+v", got)
	}
	if got := SetColumnReference(nil, "a.b.c.d"); got.RefColumn != "a.b.c.d" {
		t.Fatalf("SetColumnReference fallback failed: %+v", got)
	}

	if len(lx.errs) == 0 {
		t.Fatalf("expected lexer errors for invalid create-table branches")
	}
}

func TestStmtCoverage_FormatColumnOptionsForAlter(t *testing.T) {
	if got := formatColumnOptionsForAlter(nil); got != "" {
		t.Fatalf("expected empty for nil opts, got %q", got)
	}
	if got := formatColumnOptionsForAlter(&ColumnOption{}); got != "" {
		t.Fatalf("expected empty for blank opts, got %q", got)
	}
	if got := formatColumnOptionsForAlter(&ColumnOption{
		PrimaryKey:    true,
		Encode:        "delta",
		Compress:      "lz4",
		CompressLevel: "high",
		HasRef:        true,
		RefColumn:     "c1",
	}); !strings.Contains(got, "primary key") || !strings.Contains(got, "encode 'delta'") || !strings.Contains(got, "compress 'lz4'") || !strings.Contains(got, "level 'high'") || !strings.Contains(got, "from c1") {
		t.Fatalf("unexpected column options format: %q", got)
	}
	if got := formatColumnOptionsForAlter(&ColumnOption{
		HasRef:    true,
		RefTable:  "t1",
		RefColumn: "c1",
	}); !strings.Contains(got, "from t1.c1") {
		t.Fatalf("expected table-qualified from ref, got %q", got)
	}
	if got := formatColumnOptionsForAlter(&ColumnOption{
		HasRef:    true,
		RefDB:     "db1",
		RefTable:  "t1",
		RefColumn: "c1",
	}); !strings.Contains(got, "from db1.t1.c1") {
		t.Fatalf("expected db-qualified from ref, got %q", got)
	}
	if got := formatColumnOptionsForAlter(&ColumnOption{HasRef: true}); got != "" {
		t.Fatalf("expected empty for ref without target, got %q", got)
	}
}

func TestStmtCoverage_MiscConstructorsAndMarkers(t *testing.T) {
	lx := &errCollectLexer{}

	if NewCreateComponentNodeStmt(lx, QUERY_NODE_CREATE_QNODE_STMT, tokBytes("x")) != nil {
		t.Fatalf("invalid component dnode should fail")
	}
	if st := NewCreateComponentNodeStmt(lx, QUERY_NODE_CREATE_SNODE_STMT, tokBytes("1")); st == nil {
		t.Fatalf("valid create component failed")
	} else {
		st.iStatement()
	}
	if st := NewDropComponentNodeStmt(lx, QUERY_NODE_DROP_MNODE_STMT, tokBytes("1")); st == nil {
		t.Fatalf("valid drop component failed")
	} else {
		st.iStatement()
	}
	if st := NewRestoreComponentNodeStmt(lx, QUERY_NODE_RESTORE_VNODE_STMT, tokBytes("1")); st == nil {
		t.Fatalf("valid restore component failed")
	} else {
		st.iStatement()
	}
	bopts := CreateDefaultBnodeOptions()
	if got := SetBnodeOption(bopts, "PROTOCOL", "x"); got.ProtoStr != "x" {
		t.Fatalf("SetBnodeOption protocol failed")
	}
	if got := SetBnodeOption(bopts, "OTHER", "x"); got.ProtoStr != "" {
		t.Fatalf("SetBnodeOption non-protocol should keep proto")
	}
	if st := NewCreateBnodeStmt(lx, tokBytes("1"), bopts); st == nil {
		t.Fatalf("NewCreateBnodeStmt failed")
	} else {
		st.iStatement()
	}
	if st := NewDropBnodeStmt(lx, tokBytes("1")); st == nil {
		t.Fatalf("NewDropBnodeStmt failed")
	} else {
		st.iStatement()
	}

	if st := NewCreateDnodeStmt(lx, tokBytes("host"), nil); st == nil || st.Port != -1 {
		t.Fatalf("NewCreateDnodeStmt nil-port failed: %+v", st)
	}
	badPort := tokBytes("x")
	if NewCreateDnodeStmt(lx, tokBytes("host"), &badPort) != nil {
		t.Fatalf("NewCreateDnodeStmt bad port should fail")
	}
	if st := NewDropDnodeStmt(lx, Token{Type: INTEGRALVALUE, Bytes: []byte("2")}, true, true); st == nil || st.DnodeId != 2 {
		t.Fatalf("NewDropDnodeStmt numeric failed: %+v", st)
	}
	if st := NewDropDnodeStmt(lx, tokBytes("host"), false, false); st == nil || st.Fqdn != "host" {
		t.Fatalf("NewDropDnodeStmt fqdn failed: %+v", st)
	}
	if NewDropDnodeStmt(lx, Token{Type: INTEGRALVALUE, Bytes: []byte("x")}, false, false) != nil {
		t.Fatalf("NewDropDnodeStmt bad numeric should fail")
	}
	if st := NewAlterDnodeStmt(lx, nil, tokBytes("dnodes"), nil); st == nil || st.DnodeId != -1 {
		t.Fatalf("NewAlterDnodeStmt all-dnodes failed: %+v", st)
	}
	idTok := tokBytes("3")
	if st := NewAlterDnodeStmt(lx, &idTok, tokBytes("k"), &Token{Bytes: []byte("v")}); st == nil || st.DnodeId != 3 || st.Value != "v" {
		t.Fatalf("NewAlterDnodeStmt with value failed: %+v", st)
	}
	badIDTok := tokBytes("x")
	if NewAlterDnodeStmt(lx, &badIDTok, tokBytes("k"), nil) != nil {
		t.Fatalf("NewAlterDnodeStmt bad id should fail")
	}
	if st := NewRestoreDnodeStmt(lx, tokBytes("9")); st == nil {
		t.Fatalf("NewRestoreDnodeStmt failed")
	} else {
		st.iStatement()
	}
	if NewRestoreDnodeStmt(lx, tokBytes("x")) != nil {
		t.Fatalf("NewRestoreDnodeStmt bad id should fail")
	}
	NewCreateEncryptKeyStmt(lx, tokBytes("k")).iStatement()
	ac := NewAlterClusterStmt(lx, tokBytes("cfg"), nil)
	ac.iStatement()
	al := NewAlterLocalStmt(lx, tokBytes("cfg"), nil)
	al.iStatement()

	ss := NewStreamStmt("drop", []string{"s1", "s2"}, true, true)
	ss.iStatement()
	b := newTB()
	ss.Format(b)
	if got := b.String(); !strings.Contains(got, "drop stream if exists ignore untreated") {
		t.Fatalf("unexpected stream format: %q", got)
	}
	invokeStatementMarker(NewRecalculateStreamStmt("db1.s1", StreamRecalculateRange{From: "1", To: "2"}))

	sc := NewScanStmt("other", "x", "1", "2")
	sc.iStatement()
	b = newTB()
	sc.Format(b)
	if got := b.String(); got != "scan other x start with 1 end with 2" {
		t.Fatalf("unexpected scan format: %q", got)
	}
	cp := NewCompactStmt("other", "x", "1", "2", true, true)
	cp.iStatement()
	b = newTB()
	cp.Format(b)
	if got := b.String(); !strings.Contains(got, "compact other x") || !strings.Contains(got, "meta_only") || !strings.Contains(got, "force") {
		t.Fatalf("unexpected compact format: %q", got)
	}

	topt := CreateDefaultTokenOptions()
	if got := MergeTokenOptions(lx, nil, nil); got == nil {
		t.Fatalf("MergeTokenOptions nil,nil should return default")
	}
	if got := MergeTokenOptions(lx, nil, topt); got != topt {
		t.Fatalf("MergeTokenOptions lhs nil should return rhs")
	}
	if got := MergeTokenOptions(lx, topt, nil); got != topt {
		t.Fatalf("MergeTokenOptions rhs nil should return lhs")
	}
	topt.SetEnable(lx, tokBytes("x"))
	topt.SetTTL(lx, tokBytes("x"))
	topt.SetEnable(lx, tokBytes("0"))
	topt.SetTTL(lx, tokBytes("2"))
	topt.SetProvider(lx, tokBytes("p"))
	topt.SetExtraInfo(lx, tokBytes("e"))
	ct := NewCreateTokenStmt(lx, tokBytes("tk"), "u", topt, true)
	ct.iStatement()
	at := NewAlterTokenStmt(lx, tokBytes("tk"), nil)
	at.iStatement()
	dt := NewDropTokenStmt(lx, tokBytes("tk"), true)
	dt.iStatement()

	m := NewMultiCreateTableStmt("db.t1", "db.st", true, nil, []string{"1"}, &TableOptions{TTL: 1})
	m.iStatement()
	_ = AppendMultiCreateTableStmt(nil, "db.t2", "db.st", false, nil, []string{"2"}, nil)
	_ = AppendMultiCreateTableStmt(m, "db.t3", "db.st", true, []string{"tbname"}, []string{"3"}, nil)
	drop := NewDropTableStmt("table", true, "  if exists db.t1 ")
	drop.iStatement()
	vf := NewCreateSubTableFromFileStmt(true, " db.st ", nil, "f")
	vf.iStatement()
	vs := NewCreateVSubTableStmt(true, " db.t ", " db.st ", []string{"a"}, []string{"db.st.tg"}, []string{"1", "'x'"})
	vs.iStatement()
	alt := NewAlterTableStmt("table", " add column c1 int ")
	alt.iStatement()
	adb := NewAlterDatabaseStmt("db1", nil)
	adb.iStatement()
	r := NewRollupStmt("database", "db1", "1", "2")
	r.iStatement()

	if NewCreateMountStmt(lx, tokBytes("m1"), tokBytes("x"), tokBytes("/p"), true).DnodeID != -1 {
		t.Fatalf("invalid mount dnode should be -1")
	}
	cm := NewCreateMountStmt(lx, tokBytes("m1"), tokBytes("2"), tokBytes("/p"), true)
	cm.iStatement()
	dm := NewDropMountStmt(tokBytes("m1"), true)
	dm.iStatement()

	xn := NewXnodeStmt("create", "resource", "ep", 3, true, "u", "p")
	xn.iStatement()
	xn.TaskFrom = "1"
	xn.TaskTo = "2"
	xn.TaskOptions = "x=1"
	b = newTB()
	xn.Format(b)
	if got := b.String(); !strings.Contains(got, "create xnode") || !strings.Contains(got, "with x=1") {
		t.Fatalf("unexpected xnode format: %q", got)
	}

	g := &GrantStmt{OptrType: 1, PrivilegeName: "", Privileges: PrivSetArgs{PrivArgs: PRIV_CM_ALL, ObjType: PRIV_OBJ_TBL}, ObjName: "db1", TabName: "t1", Principal: "u1", Cond: &RawExpr{Kind: "binary", Op: Token{Type: '+'}, Left: &SQLVal{Type: IntVal, Val: []byte("1")}, Right: &SQLVal{Type: IntVal, Val: []byte("2")}}}
	invokeStatementMarker(g)
	b = newTB()
	g.Format(b)
	if got := b.String(); !strings.Contains(got, "revoke all") || !strings.Contains(got, "from u1") {
		t.Fatalf("unexpected grant format: %q", got)
	}
	if err := g.walkSubtree(func(SQLNode) (bool, error) { return false, errors.New("stop") }); err == nil {
		t.Fatalf("expected walkSubtree to propagate error")
	}

	ins := InsertStatement{InsertNode{TableName: &TableName{Name: NewTableIdent("t1")}, Values: [][]*SQLVal{{{Type: IntVal, Val: []byte("1")}}}}}
	invokeStatementMarker(ins)
	if err := ins.walkSubtree(func(SQLNode) (bool, error) { return false, errors.New("stop") }); err == nil {
		t.Fatalf("expected insert walk error")
	}

	if len(lx.errs) == 0 {
		t.Fatalf("expected lexer errors for invalid constructor branches")
	}
}

func TestStmtCoverage_UserRemainingSetterBranches(t *testing.T) {
	lx := &errCollectLexer{}
	u := &UserOptions{}

	u.setUserOptionsTotpseed(lx, Token{Bytes: nil})
	u.setUserOptionsTotpseed(lx, tokBytes("seed"))
	u.setEnable(lx, tokBytes("1"))
	u.setEnable(lx, tokBytes("x"))
	u.setSysinfo(lx, tokBytes("1"))
	u.setSysinfo(lx, tokBytes("x"))
	u.setIsImport(lx, tokBytes("1"))
	u.setIsImport(lx, tokBytes("x"))
	u.setCreatedb(lx, tokBytes("1"))
	u.setCreatedb(lx, tokBytes("x"))
	u.setChangepass(lx, tokBytes("1"))
	u.setChangepass(lx, tokBytes("x"))

	u.setSessionPerUser(lx, Token{Type: DEFAULT})
	u.setSessionPerUser(lx, Token{Type: UNLIMITED})
	u.setSessionPerUser(lx, tokBytes("3"))
	u.setSessionPerUser(lx, tokBytes("x"))

	u.setConnectTime(lx, Token{Type: DEFAULT})
	u.setConnectTime(lx, Token{Type: UNLIMITED})
	u.setConnectTime(lx, tokBytes("3"))
	u.setConnectTime(lx, tokBytes("x"))

	u.setConnectIdleTime(lx, Token{Type: DEFAULT})
	u.setConnectIdleTime(lx, Token{Type: UNLIMITED})
	u.setConnectIdleTime(lx, tokBytes("3"))
	u.setConnectIdleTime(lx, tokBytes("x"))

	u.setPasswordLifeTime(lx, Token{Type: DEFAULT})
	u.setPasswordLifeTime(lx, Token{Type: UNLIMITED})
	u.setPasswordLifeTime(lx, tokBytes("3"))
	u.setPasswordLifeTime(lx, tokBytes("x"))

	u.setPasswordReuseTime(lx, Token{Type: DEFAULT})
	u.setPasswordReuseTime(lx, Token{Type: UNLIMITED})
	u.setPasswordReuseTime(lx, tokBytes("3"))
	u.setPasswordReuseTime(lx, tokBytes("x"))

	u.setPasswordLockTime(lx, Token{Type: DEFAULT})
	u.setPasswordLockTime(lx, Token{Type: UNLIMITED})
	u.setPasswordLockTime(lx, tokBytes("3"))
	u.setPasswordLockTime(lx, tokBytes("x"))

	u.setPasswordGraceTime(lx, Token{Type: DEFAULT})
	u.setPasswordGraceTime(lx, Token{Type: UNLIMITED})
	u.setPasswordGraceTime(lx, tokBytes("3"))
	u.setPasswordGraceTime(lx, tokBytes("x"))

	u.setInactiveAccountTime(lx, Token{Type: DEFAULT})
	u.setInactiveAccountTime(lx, Token{Type: UNLIMITED})
	u.setInactiveAccountTime(lx, tokBytes("3"))
	u.setInactiveAccountTime(lx, tokBytes("x"))

	u.setAllowTokenNum(lx, Token{Type: DEFAULT})
	u.setAllowTokenNum(lx, Token{Type: UNLIMITED})
	u.setAllowTokenNum(lx, tokBytes("3"))
	u.setAllowTokenNum(lx, tokBytes("x"))

	u.setUserOptionsPassword(lx, Token{Bytes: nil})
	u.setUserOptionsPassword(lx, tokBytes("p"))

	// Hit deep merge branches in one shot.
	host, _ := NewIpRange([]byte("10.0.0.0/24"), 0)
	base := &UserOptions{
		HasPassword:            true,
		HasTotpseed:            true,
		HasEnable:              true,
		HasSysinfo:             true,
		HasIsImport:            true,
		HasCreatedb:            true,
		HasChangepass:          true,
		HasSessionPerUser:      true,
		HasConnectTime:         true,
		HasConnectIdleTime:     true,
		HasCallPerSession:      true,
		HasVnodePerCall:        true,
		HasFailedLoginAttempts: true,
		HasPasswordLifeTime:    true,
		HasPasswordReuseTime:   true,
		HasPasswordReuseMax:    true,
		HasPasswordLockTime:    true,
		HasPasswordGraceTime:   true,
		HasInactiveAccountTime: true,
		HasAllowTokenNum:       true,
		Password:               "p",
		Totpseed:               "seed",
		Enable:                 1,
		Sysinfo:                1,
		IsImport:               1,
		Createdb:               1,
		Changepass:             1,
		SessionPerUser:         1,
		ConnectTime:            60,
		ConnectIdleTime:        60,
		CallPerSession:         1,
		VnodePerCall:           1,
		FailedLoginAttempts:    1,
		PasswordLifeTime:       86400,
		PasswordReuseTime:      86400,
		PasswordReuseMax:       1,
		PasswordLockTime:       60,
		PasswordGraceTime:      86400,
		InactiveAccountTime:    86400,
		AllowTokenNum:          1,
		IpRanges:               []*IpRange{host},
		DropIpRanges:           []*IpRange{host},
		TimeRanges:             []*DateTimeRange{{Duration: 1}},
		DropTimeRanges:         []*DateTimeRange{{Duration: 1}},
	}
	m := MergeUserOptions(lx, CreateDefaultUserOptions(), base)
	if !m.HasAllowTokenNum || len(m.DropTimeRanges) != 1 {
		t.Fatalf("unexpected deep merge result: %+v", m)
	}

	au := &AlterUserStmt{
		UserName: "u2",
		UserOptions: &UserOptions{
			HasTotpseed:    true,
			Totpseed:       "abc",
			IpRanges:       []*IpRange{{Neg: 0, IPNet: host.IPNet}},
			DropIpRanges:   []*IpRange{{Neg: 0, IPNet: host.IPNet}},
			TimeRanges:     []*DateTimeRange{{Neg: 0, Duration: 10}},
			DropTimeRanges: []*DateTimeRange{{Neg: 0, Duration: 20}},
		},
	}
	invokeStatementMarker(au)
	buf := newTB()
	au.Format(buf)
	if got := buf.String(); !strings.Contains(got, "totpseed 'abc'") || !strings.Contains(got, "add host") || !strings.Contains(got, "drop allow_datetime") {
		t.Fatalf("unexpected alter-user allow branches: %q", got)
	}

	if len(lx.errs) == 0 {
		t.Fatalf("expected invalid setter errors")
	}
}

func TestStmtCoverage_RemainingMiscBranches(t *testing.T) {
	lx := &errCollectLexer{}

	// Component constructor error branches.
	if NewDropComponentNodeStmt(lx, QUERY_NODE_DROP_QNODE_STMT, tokBytes("x")) != nil {
		t.Fatalf("invalid drop component should fail")
	}
	if NewRestoreComponentNodeStmt(lx, QUERY_NODE_RESTORE_QNODE_STMT, tokBytes("x")) != nil {
		t.Fatalf("invalid restore component should fail")
	}
	if NewCreateBnodeStmt(lx, tokBytes("x"), CreateDefaultBnodeOptions()) != nil {
		t.Fatalf("invalid create bnode should fail")
	}
	if NewDropBnodeStmt(lx, tokBytes("x")) != nil {
		t.Fatalf("invalid drop bnode should fail")
	}
	cb := &CreateBnodeStmt{DnodeId: 1, Options: BnodeOptions{}}
	invokeStatementMarker(cb)
	tb := newTB()
	cb.Format(tb)
	if got := tb.String(); got != "create bnode on dnode 1" {
		t.Fatalf("unexpected create bnode format without protocol: %q", got)
	}

	// CreateTable format-dataType uncovered branch: decimal without scale.
	if got := formatDataType(DataType{Type: TSDB_DATA_TYPE_DECIMAL, Precision: 10}); got != "decimal(10)" {
		t.Fatalf("unexpected decimal without scale format: %q", got)
	}
	// Additional SetTableOption invalid parsing branches.
	opts := &TableOptions{}
	if SetTableOption(lx, opts, TABLE_OPTION_WATERMARK, [][]byte{[]byte("x")}) != nil {
		t.Fatalf("invalid watermark should fail")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_DELETE_MARK, [][]byte{[]byte("x")}) != nil {
		t.Fatalf("invalid delete mark should fail")
	}
	if SetTableOption(lx, opts, TABLE_OPTION_KEEP, []byte("x")) != nil {
		t.Fatalf("invalid keep []byte should fail")
	}

	// Scan/compact uncovered formatting branches.
	sdb := &ScanStmt{Scope: "database", Name: "db1"}
	invokeStatementMarker(sdb)
	tb = newTB()
	sdb.Format(tb)
	if got := tb.String(); got != "scan database db1" {
		t.Fatalf("unexpected scan database format: %q", got)
	}
	cvg := &CompactStmt{Scope: "vgroups"}
	invokeStatementMarker(cvg)
	tb = newTB()
	cvg.Format(tb)
	if got := tb.String(); got != "compact vgroups in (1)" {
		t.Fatalf("unexpected compact vgroups format: %q", got)
	}

	// Grant format branches: DB object and default privilege/read with grant direction.
	g := &GrantStmt{OptrType: 0, Privileges: PrivSetArgs{PrivArgs: 999, ObjType: PRIV_OBJ_DB}, ObjName: "db1", Principal: "u1"}
	invokeStatementMarker(g)
	tb = newTB()
	g.Format(tb)
	if got := tb.String(); !strings.Contains(got, "grant read on database db1 to u1") {
		t.Fatalf("unexpected grant db/read format: %q", got)
	}

	// Insert helpers uncovered branches.
	if got := formatInsertIdentifier([]byte("UPPER")); got != "`UPPER`" {
		t.Fatalf("unexpected insert identifier quote: %q", got)
	}
	empty := InsertStatement{}
	invokeStatementMarker(empty)
	tb = newTB()
	empty.Format(tb)
	if got := tb.String(); got != "insert" {
		t.Fatalf("unexpected empty insert format: %q", got)
	}
	nilIns := InsertStatement(nil)
	tb = newTB()
	nilIns.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil insert format: %q", got)
	}

	// Table DDL format branches.
	dd := &DropTableStmt{Kind: "table", WithKeyword: false}
	invokeStatementMarker(dd)
	tb = newTB()
	dd.Format(tb)
	if got := tb.String(); got != "drop table" {
		t.Fatalf("unexpected drop table format without entries: %q", got)
	}
	mc := &MultiCreateTableStmt{Entries: []MultiCreateTableEntry{
		{
			NotExists:    true,
			Target:       "db.t",
			Using:        "db.st",
			SpecificCols: []string{"c1"},
			TagValues:    []string{"1"},
			Options: &TableOptions{
				Comment:    "x",
				MaxDelay:   []tool.TDDuration{tool.NewTDDuration(2 * time.Second)},
				Watermark:  []tool.TDDuration{tool.NewTDDuration(24 * time.Hour), tool.NewTDDuration(48 * time.Hour)},
				DeleteMark: []tool.TDDuration{tool.NewTDDuration(72 * time.Hour)},
				TTL:        3,
				Keep:       []tool.TDDuration{tool.NewTDDuration(24 * time.Hour)},
				RollupFuncs: []string{
					"first",
					"last",
				},
				SMA:        []string{"c1", "c2"},
				VirtualStb: true,
			},
		},
		{Target: "db.t2", Using: "db.st2"},
	}}
	invokeStatementMarker(mc)
	tb = newTB()
	mc.Format(tb)
	if got := tb.String(); !strings.Contains(got, "create table if not exists db.t using db.st (c1) tags (1)") || !strings.Contains(got, "max_delay 2s") || !strings.Contains(got, "watermark 1d,2d") || !strings.Contains(got, "delete_mark 3d") || !strings.Contains(got, "keep 1d") || !strings.Contains(got, "rollup(first,last)") || !strings.Contains(got, "sma(c1,c2)") || !strings.Contains(got, "create table if not exists") || !strings.Contains(got, "db.t2 using db.st2") {
		t.Fatalf("unexpected multict format: %q", got)
	}
	var nilMC *MultiCreateTableStmt
	tb = newTB()
	nilMC.Format(tb)
	if got := tb.String(); got != "" {
		t.Fatalf("unexpected nil multict format: %q", got)
	}

	// Anode ctor branches.
	badAnode := tokBytes("x")
	if NewUpdateAnodeStmt(lx, &badAnode) != nil {
		t.Fatalf("invalid update anode id should fail")
	}
	if NewDropAnodeStmt(lx, &badAnode, false) != nil {
		t.Fatalf("invalid drop anode id should fail")
	}

	if len(lx.errs) == 0 {
		t.Fatalf("expected lexer errors for remaining branches")
	}
}
