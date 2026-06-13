package sqlparser

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestStatementTypeCoverage_All(t *testing.T) {
	expected := expectedStatementTypes()

	seen := map[string]struct{}{}
	for _, sql := range gatherGlobalPositiveSQLFromTests(t) {
		stmt, err := Parse(sql)
		if err != nil {
			continue
		}
		seen[reflect.TypeOf(stmt).String()] = struct{}{}
	}

	reParseCallSQL := regexp.MustCompile(`Parse\(\s*"((?:[^"\\]|\\.)*)"\s*\)`)
	reSQLField := regexp.MustCompile(`sql:\s*"((?:[^"\\]|\\.)*)"`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cwd failed: %v", err)
	}
	trySQL := func(raw string) {
		sql, err := strconv.Unquote(`"` + raw + `"`)
		if err != nil {
			return
		}
		sql = strings.TrimSpace(sql)
		if sql == "" {
			return
		}
		stmt, err := Parse(sql)
		if err != nil {
			return
		}
		seen[reflect.TypeOf(stmt).String()] = struct{}{}
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s failed: %v", name, err)
		}
		body := string(b)
		for _, re := range []*regexp.Regexp{reSQLField, reParseCallSQL} {
			matches := re.FindAllStringSubmatch(body, -1)
			for _, m := range matches {
				if len(m) != 2 {
					continue
				}
				trySQL(m[1])
			}
		}
	}

	// Supplemental seeds for rarer statement families to guarantee type hit.
	seeds := []string{
		"create bnode on dnode 1;",
		"drop bnode on dnode 1;",
		"create qnode on dnode 1;",
		"drop qnode on dnode 1;",
		"restore qnode on dnode 1;",
		"create mount if not exists m1 on dnode 1 from '/tmp/x';",
		"drop mount if exists m1;",
		"drop function if exists f1;",
		"insert into t values(1);",
		"delete from db1.t1;",
		"drop view if exists db1.v1;",
	}
	for _, sql := range seeds {
		stmt, err := Parse(sql)
		if err != nil {
			continue
		}
		seen[reflect.TypeOf(stmt).String()] = struct{}{}
	}

	var missing []string
	for _, typ := range expected {
		if _, ok := seen[typ]; !ok {
			missing = append(missing, typ)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("missing statement parse coverage for types: %v", missing)
	}
}
