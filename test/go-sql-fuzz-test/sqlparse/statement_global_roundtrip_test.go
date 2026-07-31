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

var (
	reParseCallSQL = regexp.MustCompile(`Parse\(\s*"((?:[^"\\]|\\.)*)"\s*\)`)
	reSQLField     = regexp.MustCompile(`sql:\s*"((?:[^"\\]|\\.)*)"`)
	reNegativeHint = regexp.MustCompile(`(?i)expected parse error|expected parse failure|err == nil|wantErr:\s*true|expectErr`)
)

func unescapeGoStringLiteralContent(s string) (string, bool) {
	v, err := strconv.Unquote(`"` + s + `"`)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(v), true
}

func looksLikeSQL(s string) bool {
	if s == "" {
		return false
	}
	low := strings.ToLower(strings.TrimSpace(s))
	heads := []string{
		"select ", "insert ", "create ", "alter ", "drop ", "show ", "use ",
		"grant ", "revoke ", "kill ", "flush ", "trim ", "balance ", "restore ",
		"merge ", "split ", "assign ", "recalculate ", "explain ", "describe ",
		"ssmigrate ", "compact ", "scan ", "update ", "delete ", "reset ", "reload ",
	}
	for _, h := range heads {
		if strings.HasPrefix(low, h) {
			return true
		}
	}
	return false
}

func gatherGlobalPositiveSQLFromTests(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cwd failed: %v", err)
	}

	seen := map[string]struct{}{}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s failed: %v", name, err)
		}
		body := string(data)
		// Skip files that intentionally focus on parse-failure behavior.
		if reNegativeHint.MatchString(body) {
			continue
		}

		for _, re := range []*regexp.Regexp{reSQLField, reParseCallSQL} {
			matches := re.FindAllStringSubmatch(body, -1)
			for _, m := range matches {
				if len(m) != 2 {
					continue
				}
				sql, ok := unescapeGoStringLiteralContent(m[1])
				if !ok || !looksLikeSQL(sql) {
					continue
				}
				if _, ok := seen[sql]; ok {
					continue
				}
				seen[sql] = struct{}{}
				out = append(out, sql)
			}
		}
	}

	// Merge existing dedicated corpora/suites.
	for _, sql := range gatherRoundTripSQLFromCommandTests(t) {
		if _, ok := seen[sql]; ok {
			continue
		}
		seen[sql] = struct{}{}
		out = append(out, sql)
	}
	for _, sql := range gatherRoundTripSQLFromValidCorpus(t) {
		if _, ok := seen[sql]; ok {
			continue
		}
		seen[sql] = struct{}{}
		out = append(out, sql)
	}
	for _, sql := range gatherRoundTripSQLFromPassReport(t) {
		if _, ok := seen[sql]; ok {
			continue
		}
		seen[sql] = struct{}{}
		out = append(out, sql)
	}

	return out
}

func TestStatementRoundTrip_GlobalAllSyntax(t *testing.T) {
	sqls := gatherGlobalPositiveSQLFromTests(t)
	if len(sqls) == 0 {
		t.Fatalf("no sql collected for global round-trip")
	}

	typeCount := map[string]int{}
	for i, sql := range sqls {
		stmt, err := Parse(sql)
		if err != nil {
			t.Fatalf("global parse failed idx=%d sql=%q err=%v", i, sql, err)
		}
		typeCount[reflect.TypeOf(stmt).String()]++
		runStatementRoundTrip(t, sql)
	}

	if len(typeCount) < 20 {
		keys := make([]string, 0, len(typeCount))
		for k := range typeCount {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("global type coverage too low: got=%d types=%v", len(typeCount), keys)
	}
}
