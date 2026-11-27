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
	"bytes"
	"io"
	"os"
	"regexp"
	"strconv"
	"sync"
)

var (
	parseTraceMu sync.Mutex
	reduceLineRE = regexp.MustCompile(`reduce ([0-9]+) in:`)
)

// collectReductions parses sql via goyacc parser in trace mode and extracts
// reduced production ids from debug output. It is best-effort.
func collectReductions(sql string) ([]int, error) {
	scanner := NewScanner(sql)
	reductions, code := parseWithReductionTrace(scanner)
	if code != 0 || scanner.lastErr != nil {
		return nil, scanner.lastErr
	}
	return reductions, nil
}

func parseWithReductionTrace(scanner *Scanner) ([]int, int) {
	if scanner == nil {
		return nil, 1
	}
	parseTraceMu.Lock()
	defer parseTraceMu.Unlock()

	prevDebug := yyDebug
	yyDebug = 2
	defer func() {
		yyDebug = prevDebug
	}()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return nil, yyParse(scanner)
	}
	os.Stdout = w

	traceCh := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		traceCh <- buf.String()
	}()

	code := yyParse(scanner)
	_ = w.Close()
	os.Stdout = oldStdout
	traceText := <-traceCh
	_ = r.Close()

	return parseReductionIDs(traceText), code
}

func parseReductionIDs(trace string) []int {
	matches := reduceLineRE.FindAllStringSubmatch(trace, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		id, err := strconv.Atoi(m[1])
		if err != nil || id <= 0 {
			continue
		}
		out = append(out, id)
	}
	return out
}
