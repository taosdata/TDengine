package workload

import (
	"fmt"

	"tdsqlsmith/internal/random"
)

type Sampler struct {
	cfg    Config
	total  int
	points []int
	names  []string
}

func NewSampler(cfg Config) (*Sampler, error) {
	s := &Sampler{cfg: cfg, points: make([]int, 0, len(Stmts)+1), names: append([]string(nil), Stmts...)}
	s.points = append(s.points, 0)
	sum := 0
	for _, st := range s.names {
		w := cfg.Weight(st)
		if w < 0 {
			return nil, fmt.Errorf("negative weight for %s", st)
		}
		sum += w
		s.points = append(s.points, sum)
	}
	if sum <= 0 {
		return nil, fmt.Errorf("total weight is zero")
	}
	s.total = sum
	return s, nil
}

func (s *Sampler) Pick(r *random.RNG) string {
	if s == nil || r == nil {
		return DMLSelect
	}
	x := r.Intn(s.total)
	for i := 0; i < len(s.names); i++ {
		if x >= s.points[i] && x < s.points[i+1] {
			return s.names[i]
		}
	}
	return DMLSelect
}

func (s *Sampler) Total() int {
	if s == nil {
		return 0
	}
	return s.total
}
