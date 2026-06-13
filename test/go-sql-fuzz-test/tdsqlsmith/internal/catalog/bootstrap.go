package catalog

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"tdsqlsmith/internal/executor"
)

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name    string
	Columns []Column
}

type Prepared struct {
	Database string
	Tables   []Table
	SetupSQL []string
}

type CleanupFunc func(context.Context)

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
	}
	return &Prepared{
		Database: db,
		Tables:   tableSchemas,
		SetupSQL: append([]string(nil), setupSQL...),
	}, cleanup, nil
}

// PrepareShared loads existing shared bootstrap database/schema without executing init DDL/DML.
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

func bootstrapCreateTableSQL(table string) string {
	defs := make([]string, 0, 20)
	for _, c := range bootstrapColumns() {
		defs = append(defs, fmt.Sprintf("%s %s", c.Name, c.Type))
	}
	return fmt.Sprintf("create table if not exists %s(%s)", table, strings.Join(defs, ", "))
}

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

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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
