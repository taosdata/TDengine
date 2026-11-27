// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
