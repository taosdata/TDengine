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
