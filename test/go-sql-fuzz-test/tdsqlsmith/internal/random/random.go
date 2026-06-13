package random

import (
	"fmt"
	"strconv"
	"strings"
)

// RNG is a deterministic splitmix64 generator with serializable state.
type RNG struct {
	state uint64
}

func New(seed uint64) *RNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &RNG{state: seed}
}

func (r *RNG) Seed(seed uint64) {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	r.state = seed
}

func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z = z ^ (z >> 31)
	return z
}

func (r *RNG) Intn(n int) int {
	if n <= 0 {
		panic("random.Intn with n <= 0")
	}
	return int(r.Uint64() % uint64(n))
}

func (r *RNG) Serialize() string {
	return fmt.Sprintf("%016x", r.state)
}

func (r *RNG) Deserialize(in string) error {
	s := strings.TrimSpace(in)
	if s == "" {
		return fmt.Errorf("empty rng state")
	}
	s = strings.TrimPrefix(s, "0x")
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		if dec, derr := strconv.ParseUint(s, 10, 64); derr == nil {
			v = dec
		} else {
			return fmt.Errorf("parse rng state %q: %w", in, err)
		}
	}
	r.state = v
	if r.state == 0 {
		r.state = 0x9e3779b97f4a7c15
	}
	return nil
}
