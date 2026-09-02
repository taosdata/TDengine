// Package impedance tracks per-production success and failure counts to blacklist grammar productions that almost always fail.
//
// Package impedance 按文法产生式跟踪成功与失败计数，以将几乎总是失败的产生式列入黑名单。
package impedance

import (
	"sort"
	"sync"
)

// Row is the aggregated bad/ok count for a single grammar production.
//
// Row 是单个文法产生式的失败/成功聚合计数。
type Row struct {
	Prod string `json:"prod"` // production name / 产生式名称
	Bad  int64  `json:"bad"`  // number of failed samples / 失败样本数
	OK   int64  `json:"ok"`   // number of successful samples / 成功样本数
}

// state holds the global, concurrency-safe per-production counters.
//
// state 持有全局的、并发安全的按产生式计数器。
type state struct {
	mu  sync.Mutex       // guards the count maps / 保护计数 map
	bad map[string]int64 // failure counts keyed by production / 以产生式为键的失败计数
	ok  map[string]int64 // success counts keyed by production / 以产生式为键的成功计数
}

// global is the package-wide impedance counter shared across callers.
//
// global 是包级别的阻抗计数器，供所有调用方共享。
var global = &state{
	bad: map[string]int64{},
	ok:  map[string]int64{},
}

// Reset clears all recorded production counts.
//
// Reset 清空所有已记录的产生式计数。
func Reset() {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.bad = map[string]int64{}
	global.ok = map[string]int64{}
}

// RecordOK increments the success count for the given production.
//
// RecordOK 为给定产生式的成功计数加一。
func RecordOK(prod string) {
	if prod == "" {
		return
	}
	global.mu.Lock()
	global.ok[prod]++
	global.mu.Unlock()
}

// RecordBad increments the failure count for the given production.
//
// RecordBad 为给定产生式的失败计数加一。
func RecordBad(prod string) {
	if prod == "" {
		return
	}
	global.mu.Lock()
	global.bad[prod]++
	global.mu.Unlock()
}

// Matched reports whether a production should still be used.
// It mirrors sqlsmith's idea: do not blacklist until enough bad samples,
// and then blacklist only very high failure-rate productions.
//
// Matched 报告某个产生式是否仍应继续使用。
// 它沿用 sqlsmith 的思路：在失败样本足够多之前不列入黑名单，
// 之后也只将失败率非常高的产生式列入黑名单。
func Matched(prod string) bool {
	global.mu.Lock()
	defer global.mu.Unlock()
	bad := global.bad[prod]
	if bad < 100 {
		return true
	}
	ok := global.ok[prod]
	rate := float64(bad) / float64(ok+bad)
	return rate <= 0.99
}

// Rows returns the recorded counts for all seen productions, sorted by production name.
//
// Rows 返回所有已见产生式的记录计数，并按产生式名称排序。
func Rows() []Row {
	global.mu.Lock()
	defer global.mu.Unlock()
	keys := map[string]struct{}{}
	for k := range global.bad {
		keys[k] = struct{}{}
	}
	for k := range global.ok {
		keys[k] = struct{}{}
	}
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	sort.Strings(list)
	out := make([]Row, 0, len(list))
	for _, k := range list {
		out = append(out, Row{Prod: k, Bad: global.bad[k], OK: global.ok[k]})
	}
	return out
}
