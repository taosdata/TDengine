package impedance

import (
	"sort"
	"sync"
)

type Row struct {
	Prod string `json:"prod"`
	Bad  int64  `json:"bad"`
	OK   int64  `json:"ok"`
}

type state struct {
	mu  sync.Mutex
	bad map[string]int64
	ok  map[string]int64
}

var global = &state{
	bad: map[string]int64{},
	ok:  map[string]int64{},
}

func Reset() {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.bad = map[string]int64{}
	global.ok = map[string]int64{}
}

func RecordOK(prod string) {
	if prod == "" {
		return
	}
	global.mu.Lock()
	global.ok[prod]++
	global.mu.Unlock()
}

func RecordBad(prod string) {
	if prod == "" {
		return
	}
	global.mu.Lock()
	global.bad[prod]++
	global.mu.Unlock()
}

// Matched mirrors sqlsmith's idea: do not blacklist until enough bad samples,
// and then blacklist only very high failure-rate productions.
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
