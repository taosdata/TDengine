// Package catalog bootstraps the TDengine schema for a fuzz run, creating the
// shared database and seed tables and introspecting the resulting schema.
//
// catalog 包为模糊测试运行初始化 TDengine 模式，创建共享数据库和种子表，
// 并对最终的模式进行自省。
package catalog

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"tdsqlsmith/internal/executor"
)

// Column is a table column name and its TDengine type.
//
// Column 表示表列的列名及其 TDengine 类型。
type Column struct {
	Name string // column name / 列名
	Type string // column type (e.g. "int", "timestamp") / 列类型（如 "int"、"timestamp"）
}

// Table is a table name and its ordered columns.
//
// Table 表示表名及其有序的列。
type Table struct {
	Name    string   // table name / 表名
	Columns []Column // columns in definition order / 按定义顺序排列的列
}

// Prepared is the result of bootstrapping: the active database, its tables, and
// the setup SQL that established it.
//
// Prepared 是初始化的结果：当前数据库、其表，以及建立它的 setup SQL。
type Prepared struct {
	Database string   // selected database name / 所选数据库名
	Tables   []Table  // introspected table schemas / 自省得到的表模式
	SetupSQL []string // SQL statements used to build the schema / 用于构建模式的 SQL 语句
}

// CleanupFunc releases resources created during bootstrap.
//
// CleanupFunc 释放初始化期间创建的资源。
type CleanupFunc func(context.Context)

// Bootstrap drops and recreates the shared database, creates the seed tables,
// inserts seed rows, and introspects the schema. It returns the prepared catalog
// and a cleanup function. The seed argument is currently unused. On a failed
// create it falls back to an existing database with the same prefix.
//
// Bootstrap 删除并重建共享数据库、创建种子表、插入种子行，并自省模式。
// 它返回准备好的目录和一个清理函数。seed 参数目前未使用。
// 创建失败时会回退到具有相同前缀的现有数据库。
func Bootstrap(ctx context.Context, exec *executor.Executor, seed int64, prefix string) (*Prepared, CleanupFunc, error) {
	_ = seed
	if prefix == "" {
		prefix = "tdsqlsmith"
	}
	db := fmt.Sprintf("%s_shared", prefix)

	dropOut := exec.Exec(ctx, fmt.Sprintf("drop database if exists %s", db))
	if dropOut.Class != executor.ClassOK {
		return nil, nil, fmt.Errorf("bootstrap drop database failed: class=%s err=%w", dropOut.Class, dropOut.Err)
	}
	createOut := exec.Exec(ctx, fmt.Sprintf("create database if not exists %s", db))
	if createOut.Class != executor.ClassOK {
		fallbackDB, fallbackErr := findFallbackDatabase(ctx, exec, prefix)
		if fallbackErr != nil {
			return nil, nil, fmt.Errorf("bootstrap create database failed: class=%s err=%w", createOut.Class, createOut.Err)
		}
		db = fallbackDB
	}
	if err := exec.UseDatabase(ctx, db); err != nil {
		return nil, nil, fmt.Errorf("bootstrap use database %s failed: %w", db, err)
	}

	setupSQL := BootstrapSetupSQL(db)
	ddl := setupSQL
	if len(ddl) >= 3 {
		ddl = ddl[3:]
	}
	for _, sqlText := range ddl {
		out := exec.Exec(ctx, sqlText)
		if out.Class != executor.ClassOK {
			return nil, nil, fmt.Errorf("bootstrap failed: %s: class=%s err=%w", sqlText, out.Class, out.Err)
		}
	}
	if err := exec.UseDatabase(ctx, db); err != nil {
		return nil, nil, fmt.Errorf("bootstrap set database failed: %w", err)
	}
	tableSchemas, err := introspectSchema(ctx, exec)
	if err != nil || len(tableSchemas) == 0 {
		tableSchemas = defaultSchema()
	}

	cleanup := func(cleanCtx context.Context) {
		_ = cleanCtx
		// Keep shared bootstrap database for reuse to avoid create/drop churn.
		// 保留共享初始化数据库以便复用，避免频繁创建/删除。
	}
	return &Prepared{
		Database: db,
		Tables:   tableSchemas,
		SetupSQL: append([]string(nil), setupSQL...),
	}, cleanup, nil
}

// PrepareShared loads existing shared bootstrap database/schema without executing init DDL/DML.
// PrepareShared loads the existing shared bootstrap database and its schema
// without running init DDL/DML, falling back to a prefix-matched database if the
// shared one is unavailable.
//
// PrepareShared 加载现有的共享初始化数据库及其模式，不执行初始化 DDL/DML；
// 若共享数据库不可用，则回退到与前缀匹配的数据库。
func PrepareShared(ctx context.Context, exec *executor.Executor, prefix string) (*Prepared, CleanupFunc, error) {
	if prefix == "" {
		prefix = "tdsqlsmith"
	}
	db := fmt.Sprintf("%s_shared", prefix)
	if err := exec.UseDatabase(ctx, db); err != nil {
		fallbackDB, fallbackErr := findFallbackDatabase(ctx, exec, prefix)
		if fallbackErr != nil {
			return nil, nil, fmt.Errorf("prepare shared use database %s failed: %w", db, err)
		}
		db = fallbackDB
		if err := exec.UseDatabase(ctx, db); err != nil {
			return nil, nil, fmt.Errorf("prepare shared use fallback database %s failed: %w", db, err)
		}
	}
	tableSchemas, err := introspectSchema(ctx, exec)
	if err != nil || len(tableSchemas) == 0 {
		tableSchemas = defaultSchema()
	}
	cleanup := func(cleanCtx context.Context) {
		_ = cleanCtx
	}
	return &Prepared{
		Database: db,
		Tables:   tableSchemas,
		SetupSQL: append([]string(nil), BootstrapSetupSQL(db)...),
	}, cleanup, nil
}

// BootstrapSetupSQL returns the ordered SQL statements that build the seed
// schema for db: drop, create, use, three create-table statements, and three
// inserts. An empty db defaults to "tdsqlsmith_shared".
//
// BootstrapSetupSQL 返回为 db 构建种子模式的有序 SQL 语句：drop、create、use、
// 三条 create-table 语句和三条 insert。db 为空时默认为 "tdsqlsmith_shared"。
func BootstrapSetupSQL(db string) []string {
	db = strings.TrimSpace(db)
	if db == "" {
		db = "tdsqlsmith_shared"
	}
	return []string{
		fmt.Sprintf("drop database if exists %s", db),
		fmt.Sprintf("create database if not exists %s", db),
		fmt.Sprintf("use %s", db),
		bootstrapCreateTableSQL("t1"),
		bootstrapCreateTableSQL("t2"),
		bootstrapCreateTableSQL("t3"),
		"insert into t1 values(now,1,10,1,2,11,111111111,222222222,1.25,2.5,12,34,7,9,true,'alpha','beta','gamma','\\x010203','POINT(1 2)',123.456789)",
		"insert into t2 values(now,2,20,3,4,21,211111111,322222222,3.5,4.75,-12,44,-7,19,false,'left','right','delta','\\x0A0B0C','POINT(2 3)',223.000001)",
		"insert into t3 values(now,3,30,5,6,31,311111111,422222222,5.75,6.125,22,54,17,29,true,'foo','bar','omega','\\x112233','POINT(3 4)',323.5)",
	}
}

// bootstrapCreateTableSQL builds a CREATE TABLE statement for the given table
// using the standard bootstrap column set.
//
// bootstrapCreateTableSQL 使用标准的初始化列集为给定表构建一条
// CREATE TABLE 语句。
func bootstrapCreateTableSQL(table string) string {
	defs := make([]string, 0, 20)
	for _, c := range bootstrapColumns() {
		defs = append(defs, fmt.Sprintf("%s %s", c.Name, c.Type))
	}
	return fmt.Sprintf("create table if not exists %s(%s)", table, strings.Join(defs, ", "))
}

// bootstrapColumns returns the fixed column set used for every seed table,
// covering each supported TDengine column type.
//
// bootstrapColumns 返回每个种子表使用的固定列集，
// 覆盖每种受支持的 TDengine 列类型。
func bootstrapColumns() []Column {
	return []Column{
		{Name: "ts", Type: "timestamp"},
		{Name: "id", Type: "int"},
		{Name: "v", Type: "int"},
		{Name: "c1", Type: "int"},
		{Name: "c2", Type: "int"},
		{Name: "u1", Type: "int unsigned"},
		{Name: "bi", Type: "bigint"},
		{Name: "ubi", Type: "bigint unsigned"},
		{Name: "f", Type: "float"},
		{Name: "d", Type: "double"},
		{Name: "si", Type: "smallint"},
		{Name: "usi", Type: "smallint unsigned"},
		{Name: "ti", Type: "tinyint"},
		{Name: "uti", Type: "tinyint unsigned"},
		{Name: "ok", Type: "bool"},
		{Name: "a", Type: "binary(32)"},
		{Name: "b", Type: "varchar(64)"},
		{Name: "n", Type: "nchar(32)"},
		{Name: "vb", Type: "varbinary(64)"},
		{Name: "geo", Type: "geometry(100)"},
		{Name: "de", Type: "decimal(18,6)"},
	}
}

// findFallbackDatabase queries SHOW DATABASES for a database matching prefix,
// preferring "<prefix>_shared" and otherwise the lexically greatest match.
//
// findFallbackDatabase 通过 SHOW DATABASES 查询与 prefix 匹配的数据库，
// 优先选择 "<prefix>_shared"，否则选择字典序最大的匹配项。
func findFallbackDatabase(ctx context.Context, exec *executor.Executor, prefix string) (string, error) {
	rows, err := exec.QueryRows(ctx, "show databases", 4096)
	if err != nil {
		return "", err
	}
	candidates := make([]string, 0, 8)
	tag := prefix + "_"
	for _, row := range rows.Rows {
		if len(row) == 0 {
			continue
		}
		name := strings.TrimSpace(row[0])
		if name == "" {
			continue
		}
		if name == prefix+"_shared" {
			return name, nil
		}
		if strings.HasPrefix(name, tag) {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no fallback database found with prefix %s", prefix)
	}
	sort.Strings(candidates)
	return candidates[len(candidates)-1], nil
}

// identRE matches valid unquoted SQL identifiers.
//
// identRE 匹配有效的未加引号的 SQL 标识符。
var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// introspectSchema reads the current database's tables and their column
// definitions via SHOW TABLES and DESCRIBE, skipping invalid identifiers and
// returning the tables sorted by name.
//
// introspectSchema 通过 SHOW TABLES 和 DESCRIBE 读取当前数据库的表及其列定义，
// 跳过无效标识符，并返回按名称排序的表。
func introspectSchema(ctx context.Context, exec *executor.Executor) ([]Table, error) {
	show, err := exec.QueryRows(ctx, "show tables", 256)
	if err != nil {
		return nil, err
	}
	out := make([]Table, 0, len(show.Rows))
	for _, row := range show.Rows {
		if len(row) == 0 {
			continue
		}
		name := strings.TrimSpace(row[0])
		if name == "" || !identRE.MatchString(name) {
			continue
		}
		desc, err := exec.QueryRows(ctx, fmt.Sprintf("describe %s", name), 512)
		if err != nil {
			continue
		}
		cols := make([]Column, 0, len(desc.Rows))
		for _, d := range desc.Rows {
			if len(d) < 2 {
				continue
			}
			col := strings.TrimSpace(d[0])
			typ := strings.TrimSpace(strings.ToLower(d[1]))
			if col == "" || typ == "" {
				continue
			}
			if !identRE.MatchString(col) {
				continue
			}
			cols = append(cols, Column{Name: col, Type: typ})
		}
		if len(cols) == 0 {
			continue
		}
		out = append(out, Table{Name: name, Columns: cols})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// defaultSchema returns the fallback schema (tables t1, t2, t3 with the standard
// bootstrap columns) used when introspection yields nothing.
//
// defaultSchema 返回回退模式（包含标准初始化列的表 t1、t2、t3），
// 在自省未得到任何结果时使用。
func defaultSchema() []Table {
	cols := bootstrapColumns()
	copyCols := func() []Column {
		out := make([]Column, len(cols))
		copy(out, cols)
		return out
	}
	return []Table{
		{Name: "t1", Columns: copyCols()},
		{Name: "t2", Columns: copyCols()},
		{Name: "t3", Columns: copyCols()},
	}
}
