package queryrules

import "sort"

type Summary struct {
	Required      int      `json:"required"`
	Hit           int      `json:"hit"`
	Missing       []string `json:"missing"`
	CoverageRatio float64  `json:"coverage_ratio"`
}

type Tracker struct {
	required []string
	reqSet   map[string]struct{}
	hits     map[string]struct{}
}

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

func (t *Tracker) IsCovered() bool {
	if t == nil {
		return true
	}
	return len(t.hits) == len(t.required)
}

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
