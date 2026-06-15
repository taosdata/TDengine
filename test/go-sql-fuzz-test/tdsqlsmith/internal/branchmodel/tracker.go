package branchmodel

// tracker.go tracks which positive and negative corpus cases have been covered
// during fuzzing and summarizes the resulting coverage. It is safe for
// concurrent use.
//
// tracker.go 跟踪在 fuzzing 过程中哪些正例和负例语料用例已被覆盖,
// 并汇总最终的覆盖情况。它可安全用于并发场景。

import (
	"sort"
	"sync"
	"time"

	"sqlparser"
)

// Tracker records, in a concurrency-safe way, which positive and negative corpus
// cases have been hit during fuzzing.
//
// Tracker 以并发安全的方式记录在 fuzzing 过程中哪些正例和负例语料用例已被命中。
type Tracker struct {
	mu sync.Mutex // guards the maps below / 保护下面的各个 map

	positive map[string]PositiveCase // required positive cases by id / 按 id 索引的必需正例
	negative map[string]NegativeCase // required negative cases by id / 按 id 索引的必需负例

	hitPositive map[string]HitInfo // covered positive cases by id / 按 id 索引的已覆盖正例
	hitNegative map[string]HitInfo // covered negative cases by id / 按 id 索引的已覆盖负例
}

// NewTracker builds a Tracker indexing the given positive and negative cases by id.
//
// NewTracker 构建一个 Tracker,按 id 索引给定的正例和负例。
func NewTracker(pos []PositiveCase, neg []NegativeCase) *Tracker {
	t := &Tracker{
		positive:    make(map[string]PositiveCase, len(pos)),
		negative:    make(map[string]NegativeCase, len(neg)),
		hitPositive: make(map[string]HitInfo, len(pos)),
		hitNegative: make(map[string]HitInfo, len(neg)),
	}
	for _, c := range pos {
		t.positive[c.ID] = c
	}
	for _, c := range neg {
		t.negative[c.ID] = c
	}
	return t
}

// TryMarkPositive checks stmt against every not-yet-hit positive case and marks
// each one it structurally matches as covered, recording sqlText and now. It
// returns the sorted ids of cases newly covered by this statement.
//
// TryMarkPositive 将 stmt 与每个尚未命中的正例进行比对,把结构上匹配的每个用例
// 标记为已覆盖,并记录 sqlText 和 now。它返回此语句新覆盖用例的已排序 id。
func (t *Tracker) TryMarkPositive(stmt sqlparser.Statement, sqlText string, now time.Time) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	hits := make([]string, 0, 2)
	for id, c := range t.positive {
		if _, ok := t.hitPositive[id]; ok {
			continue
		}
		if err := MatchPositive(stmt, c.KeyAssert); err != nil {
			continue
		}
		t.hitPositive[id] = HitInfo{
			CaseID:    id,
			SQL:       sqlText,
			At:        now,
			Rule:      c.Rule,
			BranchSig: c.BranchSig,
			Source:    c.Source,
		}
		hits = append(hits, id)
	}
	sort.Strings(hits)
	return hits
}

// IsPositiveCovered reports whether every positive case has been hit.
//
// IsPositiveCovered 报告是否每个正例都已被命中。
func (t *Tracker) IsPositiveCovered() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.hitPositive) == len(t.positive)
}

// IsNegativeCovered reports whether every negative case has been hit.
//
// IsNegativeCovered 报告是否每个负例都已被命中。
func (t *Tracker) IsNegativeCovered() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.hitNegative) == len(t.negative)
}

// Summary returns the current positive and negative coverage as a CoverageSummary.
//
// Summary 以 CoverageSummary 形式返回当前的正例和负例覆盖情况。
func (t *Tracker) Summary() CoverageSummary {
	t.mu.Lock()
	defer t.mu.Unlock()

	missing := make([]string, 0, len(t.positive)-len(t.hitPositive))
	for id := range t.positive {
		if _, ok := t.hitPositive[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)

	missingNeg := make([]string, 0, len(t.negative)-len(t.hitNegative))
	for id := range t.negative {
		if _, ok := t.hitNegative[id]; !ok {
			missingNeg = append(missingNeg, id)
		}
	}
	sort.Strings(missingNeg)

	cov := 0.0
	if len(t.positive) > 0 {
		cov = float64(len(t.hitPositive)) / float64(len(t.positive))
	}
	negCov := 0.0
	if len(t.negative) > 0 {
		negCov = float64(len(t.hitNegative)) / float64(len(t.negative))
	}

	return CoverageSummary{
		Required:       len(t.positive),
		Hit:            len(t.hitPositive),
		Missing:        missing,
		RequiredNeg:    len(t.negative),
		HitNeg:         len(t.hitNegative),
		MissingNeg:     missingNeg,
		CoverageRatio:  cov,
		NegRejectRatio: negCov,
	}
}
