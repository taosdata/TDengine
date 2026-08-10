package sqlparser

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestTableDDLAlignment_Parse(t *testing.T) {
	tests := []struct {
		sql   string
		check func(t *testing.T, stmt Statement)
	}{
		{
			sql: "create stable if not exists db1.st1 (ts timestamp, v int) tags (tag1 int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if !s.IsStable || s.IsVTable {
					t.Fatalf("unexpected stable flags: %+v", s)
				}
			},
		},
		{
			sql: "create stable if not exists db1.st2 (ts timestamp, v int) tags (tag1 int, tag2 timestamp);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if !s.IsStable || len(s.Tags) != 2 {
					t.Fatalf("unexpected stable tags: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.ttypes (c1 bool, c2 tinyint, c3 smallint, c4 bigint, c5 float, c6 double, c7 binary(8), c8 nchar(8), c9 int unsigned, c10 json, c11 varchar(8), c12 blob, c13 varbinary(8), c14 geometry(8), c15 decimal(8), c16 decimal(10,2));",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if len(s.Columns) != 16 {
					t.Fatalf("unexpected typed create table column count: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.tcolopt (ts timestamp primary key, v int composite key encode 'delta' compress 'lz4' level 'high' from db1.base.v);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if len(s.Columns) != 2 {
					t.Fatalf("unexpected create table column count: %+v", s)
				}
				if s.Columns[0].Options == nil || !s.Columns[0].Options.PrimaryKey {
					t.Fatalf("expected primary key option on first column: %+v", s.Columns[0].Options)
				}
				col2Opts := s.Columns[1].Options
				if col2Opts == nil || !col2Opts.PrimaryKey || col2Opts.Encode != "delta" || col2Opts.Compress != "lz4" || col2Opts.CompressLevel != "high" || !col2Opts.HasRef || col2Opts.RefDB != "db1" || col2Opts.RefTable != "base" || col2Opts.RefColumn != "v" {
					t.Fatalf("unexpected second column options: %+v", col2Opts)
				}
			},
		},
		{
			sql: "create table if not exists db1.topt (ts timestamp, v int) comment 'x' ttl 10 keep 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.Options == nil || s.Options.Comment != "x" || s.Options.TTL != 10 || len(s.Options.Keep) != 1 {
					t.Fatalf("unexpected create table options: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.ttag (ts timestamp, v int) tags (tag1 int, tag2 timestamp);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if len(s.Tags) != 2 || s.Options == nil {
					t.Fatalf("unexpected create table tags stmt: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.ttag2 (ts timestamp, v int) tags (tag1 int) ttl 10;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if len(s.Tags) != 1 || s.Options == nil || s.Options.TTL != 10 {
					t.Fatalf("unexpected create table tags+options stmt: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.toptd (ts timestamp, v int) keep 1d;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.Options == nil || len(s.Options.Keep) != 1 {
					t.Fatalf("unexpected create table keep duration options: %+v", s)
				}
			},
		},
		{
			sql: "create stable if not exists db1.st3 (ts timestamp, v int) tags (tag1 int) virtual 1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.Options == nil || !s.Options.VirtualStb {
					t.Fatalf("unexpected stable virtual option: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.topt2 (ts timestamp, v int) max_delay 10s watermark 1d,2d rollup(first,last) sma(v) delete_mark 7d;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.Options == nil {
					t.Fatalf("missing options: %+v", s)
				}
				if len(s.Options.MaxDelay) != 1 || len(s.Options.Watermark) != 2 || len(s.Options.RollupFuncs) != 2 || len(s.Options.SMA) != 1 || len(s.Options.DeleteMark) != 1 {
					t.Fatalf("unexpected rich table options: %+v", s.Options)
				}
			},
		},
		{
			sql: "create table if not exists db1.topt2b (ts timestamp, v int) max_delay 1s,2s;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if s.Options == nil || len(s.Options.MaxDelay) != 2 {
					t.Fatalf("unexpected max_delay list options: %+v", s)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt1 (ts timestamp, v int);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}
				if !s.IsVTable || s.IsStable {
					t.Fatalf("unexpected vtable flags: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists using db1.st1 () file 'f1';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateSubTableFromFileStmt)
				if !ok {
					t.Fatalf("expected *CreateSubTableFromFileStmt, got %T", stmt)
				}
				if !s.NotExists || s.Using != "db1.st1" || s.File != "f1" {
					t.Fatalf("unexpected create from file stmt: %+v", s)
				}
				if len(s.TagItems) != 0 {
					t.Fatalf("unexpected empty tag items: %+v", s.TagItems)
				}
			},
		},
		{
			sql: "create table if not exists using db1.st1 (tbname, qtags, c1 alias1, c2 as alias2) file 'f2';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateSubTableFromFileStmt)
				if !ok {
					t.Fatalf("expected *CreateSubTableFromFileStmt, got %T", stmt)
				}
				if !s.NotExists || s.Using != "db1.st1" || s.File != "f2" {
					t.Fatalf("unexpected create from file tag item stmt: %+v", s)
				}
				if len(s.TagItems) != 4 || s.TagItems[0] != "tbname" || s.TagItems[1] != "qtags" || s.TagItems[2] != "c1 alias1" || s.TagItems[3] != "c2 as alias2" {
					t.Fatalf("unexpected create from file tag items: %+v", s.TagItems)
				}
			},
		},
		{
			sql: "create table if not exists using db1.st1 (c1 as a1) file 'f3';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateSubTableFromFileStmt)
				if !ok {
					t.Fatalf("expected *CreateSubTableFromFileStmt, got %T", stmt)
				}
				if !s.NotExists || s.Using != "db1.st1" || s.File != "f3" {
					t.Fatalf("unexpected create from file as-alias stmt: %+v", s)
				}
				if len(s.TagItems) != 1 || s.TagItems[0] != "c1 as a1" {
					t.Fatalf("unexpected create from file alias tag items: %+v", s.TagItems)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt2 using db1.st1 tags (1, 'x');",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateVSubTableStmt)
				if !ok {
					t.Fatalf("expected *CreateVSubTableStmt, got %T", stmt)
				}
				if !s.NotExists || s.Target != "db1.vt2" || s.Using != "db1.st1" {
					t.Fatalf("unexpected create vsub stmt: %+v", s)
				}
				if len(s.SpecificCols) != 0 || len(s.RefCols) != 0 {
					t.Fatalf("unexpected create vsub optional lists: specific=%+v refs=%+v", s.SpecificCols, s.RefCols)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt2b using db1.st1 (tbname, c1) tags (1, 'x');",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateVSubTableStmt)
				if !ok {
					t.Fatalf("expected *CreateVSubTableStmt, got %T", stmt)
				}
				if !s.NotExists || s.Target != "db1.vt2b" || s.Using != "db1.st1" {
					t.Fatalf("unexpected create vsub specific cols stmt: %+v", s)
				}
				if len(s.SpecificCols) != 2 || s.SpecificCols[0] != "tbname" || s.SpecificCols[1] != "c1" || len(s.RefCols) != 0 {
					t.Fatalf("unexpected create vsub specific cols list: specific=%+v refs=%+v", s.SpecificCols, s.RefCols)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt3 (c1 from s1, c2 from s2) using db1.st1 tags (1);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateVSubTableStmt)
				if !ok {
					t.Fatalf("expected *CreateVSubTableStmt, got %T", stmt)
				}
				if !s.NotExists || s.Target != "db1.vt3" || s.Using != "db1.st1" {
					t.Fatalf("unexpected create vsub(ref) stmt: %+v", s)
				}
				if len(s.RefCols) != 2 || s.RefCols[0] != "c1 from s1" || s.RefCols[1] != "c2 from s2" {
					t.Fatalf("unexpected create vsub(ref) list: %+v", s.RefCols)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt3q (c1 from db2.st2.v1) using db1.st1 tags (1);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateVSubTableStmt)
				if !ok {
					t.Fatalf("expected *CreateVSubTableStmt, got %T", stmt)
				}
				if !s.NotExists || s.Target != "db1.vt3q" || s.Using != "db1.st1" {
					t.Fatalf("unexpected create vsub(qualified ref) stmt: %+v", s)
				}
				if len(s.RefCols) != 1 || s.RefCols[0] != "c1 from db2.st2.v1" {
					t.Fatalf("unexpected create vsub(qualified ref) list: %+v", s.RefCols)
				}
			},
		},
		{
			sql: "create vtable if not exists db1.vt4 (c1, c2) using db1.st1 tags (1);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateVSubTableStmt)
				if !ok {
					t.Fatalf("expected *CreateVSubTableStmt, got %T", stmt)
				}
				if !s.NotExists || s.Target != "db1.vt4" || s.Using != "db1.st1" {
					t.Fatalf("unexpected create vsub(list) stmt: %+v", s)
				}
				if len(s.RefCols) != 2 || s.RefCols[0] != "c1" || s.RefCols[1] != "c2" {
					t.Fatalf("unexpected create vsub(list) refs: %+v", s.RefCols)
				}
			},
		},
		{
			sql: "drop table with if exists db1.t1, db1.t2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropTableStmt)
				if !ok {
					t.Fatalf("expected *DropTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || !s.WithKeyword {
					t.Fatalf("unexpected drop table stmt: %+v", s)
				}
			},
		},
		{
			sql: "create table if not exists db1.tsub1 using db1.st1 tags (1) ttl 10 if not exists db1.tsub2 using db1.st1 tags (2) ttl 20;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*MultiCreateTableStmt)
				if !ok {
					t.Fatalf("expected *MultiCreateTableStmt, got %T", stmt)
				}
				if len(s.Entries) != 2 {
					t.Fatalf("unexpected multi create entries: %+v", s)
				}
				if s.Entries[0].Target != "db1.tsub1" || s.Entries[1].Target != "db1.tsub2" {
					t.Fatalf("unexpected multi create targets: %+v", s)
				}
				if s.Entries[0].Options == nil || s.Entries[1].Options == nil || s.Entries[0].Options.TTL != 10 || s.Entries[1].Options.TTL != 20 {
					t.Fatalf("unexpected multi create options: %+v", s.Entries)
				}
			},
		},
		{
			sql: "create table if not exists db1.tsub3 using db1.st1 tags (1) ttl 10 if not exists db1.tsub4 using db1.st1 tags (2) comment 'x';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*MultiCreateTableStmt)
				if !ok {
					t.Fatalf("expected *MultiCreateTableStmt, got %T", stmt)
				}
				if len(s.Entries) != 2 || s.Entries[0].Target != "db1.tsub3" || s.Entries[1].Target != "db1.tsub4" {
					t.Fatalf("unexpected multi create with options entries: %+v", s)
				}
				if s.Entries[0].Options == nil || s.Entries[0].Options.TTL != 10 || s.Entries[1].Options == nil || s.Entries[1].Options.Comment != "x" {
					t.Fatalf("unexpected multi create with options payload: %+v", s.Entries)
				}
			},
		},
		{
			sql: "alter table db1.t1 add column c1 int;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" {
					t.Fatalf("unexpected alter table stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 ttl 10 keep 1d;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || !strings.Contains(s.ClauseRaw, "ttl 10") || !strings.Contains(s.ClauseRaw, "keep 1d") {
					t.Fatalf("unexpected alter options stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 set tag c1='x', c2=1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || !strings.Contains(s.ClauseRaw, "c1='x'") || !strings.Contains(s.ClauseRaw, "c2=1") {
					t.Fatalf("unexpected alter set tag stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 alter column c1 set c2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected alter set ref stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 alter column c1 set t2.c2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected alter set qualified ref stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 alter column c1 set db2.t2.c2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected alter set 3-part ref stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 alter column c1 set null;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected alter set null stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 add column c3 int encode 'delta';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected add column options stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter table db1.t1 modify column c1 encode 'delta' compress 'lz4';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterTableStmt)
				if !ok {
					t.Fatalf("expected *AlterTableStmt, got %T", stmt)
				}
				if s.Kind != "table" || s.ClauseRaw == "" {
					t.Fatalf("unexpected modify column options stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter database db1 buffer 10;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterDatabaseStmt)
				if !ok {
					t.Fatalf("expected *AlterDatabaseStmt, got %T", stmt)
				}
				if s.Name != "db1" || s.Options == nil || s.Options.Buffer != 10 {
					t.Fatalf("unexpected alter database stmt: %+v", s)
				}
			},
		},
		{
			sql: "rollup database db1 start with 1 end with 2;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*RollupStmt)
				if !ok {
					t.Fatalf("expected *RollupStmt, got %T", stmt)
				}
				if s.Scope != "database" || s.Name != "db1" || s.Start != "1" || s.End != "2" {
					t.Fatalf("unexpected rollup stmt: %+v", s)
				}
			},
		},
		{
			sql: "rollup db1. vgroups in (1,2) start with 3 end with 4;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*RollupStmt)
				if !ok {
					t.Fatalf("expected *RollupStmt, got %T", stmt)
				}
				if s.Scope != "vgroups" || s.Name != "db1" || s.Start != "3" || s.End != "4" {
					t.Fatalf("unexpected rollup vgroups stmt: %+v", s)
				}
			},
		},
		{
			sql: "compact database db1 meta_only force;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CompactStmt)
				if !ok {
					t.Fatalf("expected *CompactStmt, got %T", stmt)
				}
				if s.Scope != "database" || s.Name != "db1" || !s.MetaOnly || !s.Force {
					t.Fatalf("unexpected compact stmt: %+v", s)
				}
			},
		},
		{
			sql: "compact database db1 meta_only;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CompactStmt)
				if !ok {
					t.Fatalf("expected *CompactStmt, got %T", stmt)
				}
				if s.Scope != "database" || s.Name != "db1" || !s.MetaOnly || s.Force {
					t.Fatalf("unexpected compact meta_only stmt: %+v", s)
				}
			},
		},
		{
			sql: "show user databases;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "databases" || s.DBKind != "user" {
					t.Fatalf("unexpected show user databases stmt: %+v", s)
				}
			},
		},
		{
			sql: "show system databases;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "databases" || s.DBKind != "system" {
					t.Fatalf("unexpected show system databases stmt: %+v", s)
				}
			},
		},
		{
			sql: "show normal db1. tables;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "tables" {
					t.Fatalf("unexpected show normal tables stmt: %+v", s)
				}
			},
		},
		{
			sql: "show db1. vgroups;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.Kind != "vgroups" {
					t.Fatalf("unexpected show db vgroups stmt: %+v", s)
				}
			},
		},
		{
			sql: "create index if not exists idx1 on db1.t1(c1, c2);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "index" || !s.IfNotExists || s.Name != "idx1" || s.OnTable != "db1.t1" {
					t.Fatalf("unexpected create index stmt: %+v", s)
				}
				if len(s.Columns) != 2 || s.Columns[0] != "c1" || s.Columns[1] != "c2" {
					t.Fatalf("unexpected create index columns: %+v", s.Columns)
				}
			},
		},
		{
			sql: "create sma index if not exists idxs on db1.t1 function(avg(v)) interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "sma_index" || !s.IfNotExists || s.Name != "idxs" || s.OnTable != "db1.t1" {
					t.Fatalf("unexpected create sma index stmt: %+v", s)
				}
				if s.Options != "function(avg(v)) interval(1d)" {
					t.Fatalf("unexpected create sma index options: %+v", s)
				}
			},
		},
		{
			sql: "create rsma if not exists r1 on db1.t1 function(avg(v), sum(v)) interval(1d, 2d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || !s.IfNotExists || s.Name != "r1" || s.OnTable != "db1.t1" {
					t.Fatalf("unexpected create rsma stmt: %+v", s)
				}
				if len(s.Funcs) != 2 || s.Funcs[0] != "avg(v)" || s.Funcs[1] != "sum(v)" {
					t.Fatalf("unexpected create rsma funcs: %+v", s.Funcs)
				}
				if len(s.Intervals) != 2 || s.Intervals[0] != "1d" || s.Intervals[1] != "2d" {
					t.Fatalf("unexpected create rsma intervals: %+v", s.Intervals)
				}
			},
		},
		{
			sql: "create tsma if not exists t1 on db1.t1 function(avg(v)) interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || s.IsRecursive || !s.IfNotExists || s.Name != "t1" || s.OnTable != "db1.t1" || s.Interval != "1d" {
					t.Fatalf("unexpected create tsma stmt: %+v", s)
				}
				if len(s.Funcs) != 1 || s.Funcs[0] != "avg(v)" {
					t.Fatalf("unexpected create tsma funcs: %+v", s.Funcs)
				}
			},
		},
		{
			sql: "create recursive tsma if not exists t2 on db1.t1 interval(1d);",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateNamedStmt)
				if !ok {
					t.Fatalf("expected *CreateNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || !s.IsRecursive || !s.IfNotExists || s.Name != "t2" || s.OnTable != "db1.t1" || s.Interval != "1d" {
					t.Fatalf("unexpected create recursive tsma stmt: %+v", s)
				}
				if len(s.Funcs) != 0 {
					t.Fatalf("unexpected recursive tsma funcs: %+v", s.Funcs)
				}
			},
		},
		{
			sql: "drop rsma if exists db1.r1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropNamedStmt)
				if !ok {
					t.Fatalf("expected *DropNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || s.Name != "db1.r1" || !s.IfExists {
					t.Fatalf("unexpected drop rsma stmt: %+v", s)
				}
			},
		},
		{
			sql: "drop tsma if exists db1.t1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropNamedStmt)
				if !ok {
					t.Fatalf("expected *DropNamedStmt, got %T", stmt)
				}
				if s.Kind != "tsma" || s.Name != "db1.t1" || !s.IfExists {
					t.Fatalf("unexpected drop tsma stmt: %+v", s)
				}
			},
		},
		{
			sql: "drop index if exists db1.idx1;",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropNamedStmt)
				if !ok {
					t.Fatalf("expected *DropNamedStmt, got %T", stmt)
				}
				if s.Kind != "index" || s.Name != "db1.idx1" || !s.IfExists {
					t.Fatalf("unexpected drop index stmt: %+v", s)
				}
			},
		},
		{
			sql: "alter rsma if exists db1.r1 function(avg(v),sum(v));",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterNamedStmt)
				if !ok {
					t.Fatalf("expected *AlterNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || s.Name != "db1.r1" || !s.IfExists {
					t.Fatalf("unexpected alter rsma function stmt: %+v", s)
				}
				if len(s.Funcs) != 2 || s.Funcs[0] != "avg(v)" || s.Funcs[1] != "sum(v)" {
					t.Fatalf("unexpected alter rsma function list: %+v", s.Funcs)
				}
			},
		},
		{
			sql: "alter rsma if exists db1.r1 function(avg(v));",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterNamedStmt)
				if !ok {
					t.Fatalf("expected *AlterNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || s.Name != "db1.r1" || !s.IfExists {
					t.Fatalf("unexpected alter rsma func(expr) stmt: %+v", s)
				}
				if len(s.Funcs) != 1 || s.Funcs[0] != "avg(v)" {
					t.Fatalf("unexpected alter rsma func(expr) list: %+v", s.Funcs)
				}
			},
		},
		{
			sql: "alter rsma if exists db1.r1 function();",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*AlterNamedStmt)
				if !ok {
					t.Fatalf("expected *AlterNamedStmt, got %T", stmt)
				}
				if s.Kind != "rsma" || s.Name != "db1.r1" || !s.IfExists {
					t.Fatalf("unexpected alter rsma empty function stmt: %+v", s)
				}
				if len(s.Funcs) != 0 {
					t.Fatalf("unexpected alter rsma empty function list: %+v", s.Funcs)
				}
			},
		},
		{
			sql: "scan database db1 start with timestamp '2024-01-01' end with '2024-01-02';",
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ScanStmt)
				if !ok {
					t.Fatalf("expected *ScanStmt, got %T", stmt)
				}
				if s.Scope != "database" || s.Name != "db1" || s.Start != "timestamp:2024-01-01" || s.End != "2024-01-02" {
					t.Fatalf("unexpected scan stmt bounds: %+v", s)
				}
			},
		},
	}

	for _, tt := range tests {
		stmt, err := Parse(tt.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tt.sql, err)
		}
		tt.check(t, stmt)
	}
}

func TestXnodeAlignment_Parse(t *testing.T) {
	tests := []struct {
		sql    string
		action string
		kind   string
	}{
		{sql: "create xnode task 'task-1' with parser='x';", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-1';", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-1' with parser 'x' mode=1;", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-1' with parser='x', mode=1 and enabled;", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-1' with parser='x' trigger 'cron';", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-1' with parser='x', trigger='cron';", action: "create_typed", kind: "task"},
		{sql: "create xnode task 'task-2' from 'mqtt://a' to database db1 with parser='x';", action: "create_typed_flow", kind: "task"},
		{sql: "create xnode task 'task-3' from topic tp1 to 'mqtt://b' with parser='x';", action: "create_typed_flow", kind: "task"},
		{sql: "create xnode job on 1 with config='{}';", action: "create_typed_on", kind: "job"},
		{sql: "create xnode job on 1;", action: "create_typed_on", kind: "job"},
		{sql: "rebalance xnode task 1 with xnode_id=3;", action: "rebalance", kind: "task"},
		{sql: "rebalance xnode task;", action: "rebalance_where", kind: "task"},
		{sql: "rebalance xnode task where v > 1;", action: "rebalance_where", kind: "task"},
		{sql: "alter xnode task 1;", action: "alter", kind: "task"},
		{sql: "alter xnode task 1 from 'mqtt://a' to database db1 with parser='x';", action: "alter", kind: "task"},
		{sql: "alter xnode task 'task-1' with parser='x';", action: "alter", kind: "task"},
		{sql: "drop xnode 1 force;", action: "drop", kind: ""},
	}

	for _, tt := range tests {
		stmt, err := Parse(tt.sql)
		if err != nil {
			t.Fatalf("parse failed for %q: %v", tt.sql, err)
		}
		s, ok := stmt.(*XnodeStmt)
		if !ok {
			t.Fatalf("expected *XnodeStmt for %q, got %T", tt.sql, stmt)
		}
		if s.Action != tt.action {
			t.Fatalf("unexpected action for %q: %+v", tt.sql, s)
		}
		if s.ResourceType != tt.kind {
			t.Fatalf("unexpected resource type for %q: %+v", tt.sql, s)
		}
		if tt.sql == "create xnode task 'task-2' from 'mqtt://a' to database db1 with parser='x';" {
			if s.TaskFrom != "mqtt://a" || s.TaskTo != "db1" || s.TaskOptions == "" {
				t.Fatalf("unexpected xnode task flow fields for %q: %+v", tt.sql, s)
			}
		}
		if tt.sql == "alter xnode task 1 from 'mqtt://a' to database db1 with parser='x';" {
			if s.TaskFrom != "mqtt://a" || s.TaskTo != "db1" || s.TaskOptions == "" {
				t.Fatalf("unexpected xnode alter task fields for %q: %+v", tt.sql, s)
			}
		}
	}
}

func TestXnodeAlignment_LargeParserOption(t *testing.T) {
	parser := strings.Repeat("x", 64*1024)
	stmt, err := Parse("create xnode task 'task-large' with parser='" + parser + "';")
	if err != nil {
		t.Fatalf("parse large XNODE parser option failed: %v", err)
	}

	xnode, ok := stmt.(*XnodeStmt)
	if !ok {
		t.Fatalf("expected *XnodeStmt, got %T", stmt)
	}
	if !strings.Contains(xnode.TaskOptions, parser) {
		t.Fatalf("large XNODE parser option was truncated: got %d bytes", len(xnode.TaskOptions))
	}
}

func TestXnodeAlignment_LemonParserStorage(t *testing.T) {
	cmdnodes, err := os.ReadFile("lemon/cmdnodes.h")
	if err != nil {
		t.Fatalf("read Lemon cmdnodes.h failed: %v", err)
	}
	cmdnodesText := string(cmdnodes)
	dynamicParser := regexp.MustCompile(`(?m)\bchar\s*\*\s*parser\s*;`)
	fixedParser := regexp.MustCompile(`(?m)\bchar\s+parser\s*\[`)
	if !dynamicParser.MatchString(cmdnodesText) {
		t.Fatal("Lemon SXnodeTaskOptions parser must use dynamic storage")
	}
	if fixedParser.MatchString(cmdnodesText) {
		t.Fatal("Lemon SXnodeTaskOptions parser still uses the 48 KiB fixed buffer")
	}

	creator, err := os.ReadFile("lemon/parAstCreater.c")
	if err != nil {
		t.Fatalf("read Lemon parAstCreater.c failed: %v", err)
	}
	creatorText := string(creator)
	for _, expected := range []string{
		"taosMemFreeClear(pOptions->parser)",
		"taosMemoryCalloc(1, parserCapacity + 1)",
		"pOptions->parserLen = strlen(pOptions->parser)",
		"TSDB_XNODE_TASK_PARSER_MAX_LEN",
	} {
		if !strings.Contains(creatorText, expected) {
			t.Fatalf("Lemon parser storage is missing production behavior %q", expected)
		}
	}
}

func TestTableDDLAlignment_CreateVTableMixedRefListRejected(t *testing.T) {
	_, err := Parse("create vtable if not exists db1.vtmix (c1, c2 from s2) using db1.st1 tags (1);")
	if err == nil {
		t.Fatalf("expected parse error for mixed column_ref/specific_column_ref list")
	}
}

func TestTableDDLAlignment_MultiCreateAllowsEmptyTableOptions(t *testing.T) {
	stmt, err := Parse("create table if not exists db1.tsuba using db1.st1 tags (1) if not exists db1.tsubb using db1.st1 tags (2);")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	s, ok := stmt.(*MultiCreateTableStmt)
	if !ok {
		t.Fatalf("expected *MultiCreateTableStmt, got %T", stmt)
	}
	if len(s.Entries) != 2 || s.Entries[0].Options == nil || s.Entries[1].Options == nil {
		t.Fatalf("unexpected multi create empty-options entries: %+v", s.Entries)
	}
}
