package sqlparser

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestTrackedBufferMyprintf_OnlySupportedVerbs(t *testing.T) {
	const root = "."
	callRe := regexp.MustCompile(`Myprintf\("([^"\\]*(\\.[^"\\]*)*)"`)
	allowed := map[byte]struct{}{
		'c': {},
		's': {},
		'v': {},
		'a': {},
		'%': {},
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == ".git" || path == "lemon" || path == "reports" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		matches := callRe.FindAllStringSubmatch(string(b), -1)
		for _, m := range matches {
			if len(m) < 2 {
				continue
			}
			format := m[1]
			for i := 0; i < len(format); i++ {
				if format[i] != '%' {
					continue
				}
				if i+1 >= len(format) {
					t.Fatalf("invalid Myprintf format in %s: %q", path, format)
				}
				verb := format[i+1]
				if _, ok := allowed[verb]; !ok {
					t.Fatalf("unsupported Myprintf verb in %s: format=%q verb=%%%c", path, format, verb)
				}
				i++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk source files failed: %v", err)
	}
}
