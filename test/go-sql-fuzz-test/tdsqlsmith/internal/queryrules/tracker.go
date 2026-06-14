package queryrules

// tracker.go tracks which required query rules have been hit during fuzzing and
// summarizes the resulting coverage.
//
// tracker.go 跟踪在 fuzzing 过程中哪些必需查询规则被命中,并汇总最终的覆盖情况。

import "sort"

// Summary captures query-rule coverage at a point in time.
//
// Summary 捕获某一时刻的查询规则覆盖情况。
type Summary struct {
	Required      int      `json:"required"`       // number of required rules / 必需规则数量
	Hit           int      `json:"hit"`            // number of required rules hit / 已命中的必需规则数量
	Missing       []string `json:"missing"`        // sorted names of rules not yet hit / 尚未命中的规则名(已排序)
	CoverageRatio float64  `json:"coverage_ratio"` // Hit/Required, or 0 when none required / Hit/Required,无必需规则时为 0
}

// Tracker records which required query rules have been exercised.
//
// Tracker 记录哪些必需查询规则已被覆盖。
type Tracker struct {
	required []string            // ordered list of required rule names / 必需规则名的有序列表
	reqSet   map[string]struct{} // membership set of required rules / 必需规则的成员集合
	hits     map[string]struct{} // set of required rules seen so far / 迄今已见到的必需规则集合
}

// NewTracker creates a Tracker for the given required rules, de-duplicating them
// while preserving order.
//
// NewTracker 为给定的必需规则创建一个 Tracker,在保持顺序的同时去重。
func NewTracker(required []string) *Tracker {
	uniq := uniquePreserveOrder(required)
	reqSet := make(map[string]struct{}, len(uniq))
	for _, r := range uniq {
		reqSet[r] = struct{}{}
	}
	return &Tracker{
		required: uniq,
		reqSet:   reqSet,
		hits:     make(map[string]struct{}, len(uniq)),
	}
}

// MarkMany records each rule in rules that belongs to the required set as hit.
//
// MarkMany 将 rules 中属于必需集合的每条规则标记为已命中。
func (t *Tracker) MarkMany(rules []string) {
	if t == nil {
		return
	}
	for _, r := range rules {
		if _, ok := t.reqSet[r]; !ok {
			continue
		}
		t.hits[r] = struct{}{}
	}
}

// MissingRules returns the sorted names of required rules not yet hit.
//
// MissingRules 返回尚未命中的必需规则名(已排序)。
func (t *Tracker) MissingRules() []string {
	if t == nil {
		return nil
	}
	out := make([]string, 0, len(t.required)-len(t.hits))
	for _, r := range t.required {
		if _, ok := t.hits[r]; ok {
			continue
		}
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// IsCovered reports whether every required rule has been hit. A nil Tracker is
// considered covered.
//
// IsCovered 报告是否每条必需规则都已被命中。nil 的 Tracker 视为已覆盖。
func (t *Tracker) IsCovered() bool {
	if t == nil {
		return true
	}
	return len(t.hits) == len(t.required)
}

// Summary returns the current coverage as a Summary.
//
// Summary 以 Summary 形式返回当前的覆盖情况。
func (t *Tracker) Summary() Summary {
	if t == nil {
		return Summary{}
	}
	cov := 0.0
	if len(t.required) > 0 {
		cov = float64(len(t.hits)) / float64(len(t.required))
	}
	return Summary{
		Required:      len(t.required),
		Hit:           len(t.hits),
		Missing:       t.MissingRules(),
		CoverageRatio: cov,
	}
}
