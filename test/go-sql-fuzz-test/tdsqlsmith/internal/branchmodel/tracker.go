package branchmodel

import (
	"sort"
	"strings"
	"sync"
	"time"

	"sqlparser"
)

type Tracker struct {
	mu sync.Mutex

	positive map[string]PositiveCase
	negative map[string]NegativeCase

	hitPositive map[string]HitInfo
	hitNegative map[string]HitInfo
}

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

func (t *Tracker) MarkNegativeReject(c NegativeCase, sqlText string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.hitNegative[c.ID]; ok {
		return
	}
	t.hitNegative[c.ID] = HitInfo{
		CaseID: c.ID,
		SQL:    sqlText,
		At:     now,
		Rule:   c.Rule,
		Source: "select_branch_negative",
		// BranchSig stores expected error type for negative corpus.
		BranchSig: c.ErrType,
	}
}

func (t *Tracker) IsPositiveCovered() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.hitPositive) == len(t.positive)
}

func (t *Tracker) IsNegativeCovered() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.hitNegative) == len(t.negative)
}

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

func (t *Tracker) PositiveHits() []HitInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]HitInfo, 0, len(t.hitPositive))
	for _, h := range t.hitPositive {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].CaseID, out[j].CaseID) < 0
	})
	return out
}

func (t *Tracker) MissingPositiveIDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.positive)-len(t.hitPositive))
	for id := range t.positive {
		if _, ok := t.hitPositive[id]; ok {
			continue
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (t *Tracker) PositiveCase(id string) (PositiveCase, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.positive[id]
	return c, ok
}

func (t *Tracker) NegativeHits() []HitInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]HitInfo, 0, len(t.hitNegative))
	for _, h := range t.hitNegative {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Compare(out[i].CaseID, out[j].CaseID) < 0
	})
	return out
}
