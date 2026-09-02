package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

const (
	validCount   = 300
	invalidCount = 300
)

func main() {
	root, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	outDir := filepath.Join(root, "testdata", "sql_corpus")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}

	validPath := filepath.Join(outDir, "valid_sql_cases.tsv")
	invalidPath := filepath.Join(outDir, "invalid_sql_cases.tsv")

	if err := os.WriteFile(validPath, buildValid(), 0o644); err != nil {
		panic(err)
	}
	if err := os.WriteFile(invalidPath, buildInvalid(), 0o644); err != nil {
		panic(err)
	}

	fmt.Printf("generated:\n- %s\n- %s\n", validPath, invalidPath)
}

func buildValid() []byte {
	var b bytes.Buffer
	b.WriteString("case_id\tsql\tstmt_type\tkey_assert\n")

	for i := 1; i <= validCount; i++ {
		db := fmt.Sprintf("dbv%03d", i)
		tbl := fmt.Sprintf("tv%03d", i)
		stb := fmt.Sprintf("sv%03d", i)
		id := (i % 97) + 1
		buffer := (i % 256) + 1

		switch i % 6 {
		case 0:
			sql := fmt.Sprintf("create database if not exists %s;", db)
			key := fmt.Sprintf("dbname=%s;ignoreexists=true", db)
			writeCase(&b, i, sql, "CreateDatabaseStmt", key)
		case 1:
			sql := fmt.Sprintf("alter database %s buffer %d;", db, buffer)
			key := fmt.Sprintf("name=%s;buffer=%d", db, buffer)
			writeCase(&b, i, sql, "AlterDatabaseStmt", key)
		case 2:
			sql := fmt.Sprintf("create table %s.%s(ts timestamp, v int);", db, tbl)
			key := fmt.Sprintf("table=%s;is_stable=false;tags_len=0;options_non_nil=true", tbl)
			writeCase(&b, i, sql, "CreateTableStmt", key)
		case 3:
			sql := fmt.Sprintf("create stable %s.%s(ts timestamp, v int) tags(tag1 int);", db, stb)
			key := fmt.Sprintf("table=%s;is_stable=true;tags_len=1;options_non_nil=true", stb)
			writeCase(&b, i, sql, "CreateTableStmt", key)
		case 4:
			sql := fmt.Sprintf("show %s. streams;", db)
			key := fmt.Sprintf("kind=streams;dbname=%s", db)
			writeCase(&b, i, sql, "ShowStmt", key)
		case 5:
			sql := fmt.Sprintf("show dnode %d variables;", id)
			key := fmt.Sprintf("kind=dnode_variables;hasid=true;id=%d", id)
			writeCase(&b, i, sql, "ShowStmt", key)
		}
	}

	return b.Bytes()
}

func buildInvalid() []byte {
	var b bytes.Buffer
	b.WriteString("case_id\tsql\terr_type\n")

	for i := 1; i <= invalidCount; i++ {
		db := fmt.Sprintf("dbi%03d", i)
		tbl := fmt.Sprintf("ti%03d", i)
		id := (i % 97) + 1

		switch i % 6 {
		case 0:
			writeInvalid(&b, i, "select", "incomplete")
		case 1:
			sql := fmt.Sprintf("create database %s buffer", db)
			writeInvalid(&b, i, sql, "incomplete")
		case 2:
			sql := fmt.Sprintf("create table %s.%s(ts timestamp, v int", db, tbl)
			writeInvalid(&b, i, sql, "incomplete")
		case 3:
			sql := fmt.Sprintf("show dnode %d", id)
			writeInvalid(&b, i, sql, "incomplete")
		case 4:
			sql := fmt.Sprintf("show %s streams;", db)
			writeInvalid(&b, i, sql, "syntax")
		case 5:
			sql := fmt.Sprintf("create stable %s.%s(ts timestamp, v int);", db, tbl)
			writeInvalid(&b, i, sql, "incomplete")
		}
	}

	return b.Bytes()
}

func writeCase(b *bytes.Buffer, i int, sql, typ, key string) {
	fmt.Fprintf(b, "v%03d\t%s\t%s\t%s\n", i, sql, typ, key)
}

func writeInvalid(b *bytes.Buffer, i int, sql, typ string) {
	fmt.Fprintf(b, "i%03d\t%s\t%s\n", i, sql, typ)
}
