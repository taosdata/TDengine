// Package random provides a deterministic splitmix64 random number generator with serializable state.
//
// Package random 提供一个状态可序列化的确定性 splitmix64 随机数生成器。
package random

import (
	"fmt"
	"strconv"
	"strings"
)

// RNG is a deterministic splitmix64 generator with serializable state.
//
// RNG 是一个状态可序列化的确定性 splitmix64 生成器。
type RNG struct {
	state uint64 // current internal generator state / 当前内部生成器状态
}

// New returns an RNG seeded with seed, substituting a fixed nonzero seed when seed is 0.
//
// New 返回以 seed 作为种子的 RNG，当 seed 为 0 时使用一个固定的非零种子替代。
func New(seed uint64) *RNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &RNG{state: seed}
}

// Uint64 advances the generator and returns the next pseudo-random 64-bit value.
//
// Uint64 推进生成器并返回下一个 64 位伪随机值。
func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z = z ^ (z >> 31)
	return z
}

// Intn returns a pseudo-random int in [0, n) and panics if n is not positive.
//
// Intn 返回 [0, n) 区间内的伪随机整数，若 n 不为正则触发 panic。
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		panic("random.Intn with n <= 0")
	}
	return int(r.Uint64() % uint64(n))
}

// Serialize returns the generator state as a 16-digit hexadecimal string.
//
// Serialize 将生成器状态返回为 16 位十六进制字符串。
func (r *RNG) Serialize() string {
	return fmt.Sprintf("%016x", r.state)
}

// Deserialize restores the generator state from a hex or decimal string, substituting a fixed seed if the value is 0.
//
// Deserialize 从十六进制或十进制字符串恢复生成器状态，若取值为 0 则使用一个固定种子替代。
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
