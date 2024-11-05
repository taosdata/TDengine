package process

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_i2string(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{"abc", "abc"},
		{"abcdef", "abcdef"},
		{[]byte{97, 98, 99, 100, 101, 102}, "abcdef"},
	}

	for _, tt := range tests {
		res := i2string(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_i2string_panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for unexpected type, but did not panic")
		}
	}()

	i2string(12345)
}

func Test_i2float(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected float64
	}{
		{int8(1), 1.0},
		{int16(1), 1.0},
		{int32(1), 1.0},
		{int64(1), 1.0},
		{uint8(1), 1.0},
		{uint16(1), 1.0},
		{uint32(1), 1.0},
		{uint64(1), 1.0},
		{float32(1.5), 1.5},
		{float64(1.5), 1.5},
		{true, 1.0},
		{false, 0.0},
	}

	for _, tt := range tests {
		res := i2float(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_i2float_panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for unexpected type, but did not panic")
		}
	}()

	i2float("unexpected type")
}

func Test_getRoleStr(t *testing.T) {
	tests := []struct {
		value    float64
		expected string
	}{
		{0, "offline"},
		{99.5, "follower"},
		{100, "follower"},
		{100.4, "follower"},
		{100.5, "candidate"},
		{101, "candidate"},
		{101.4, "candidate"},
		{101.5, "leader"},
		{102, "leader"},
		{102.4, "leader"},
		{102.5, "error"},
		{103, "error"},
		{104, "learner"},
		{99.4, "unknown"},
		{105, "unknown"},
		{-1, "unknown"},
		{150, "unknown"},
	}

	for _, tt := range tests {
		res := getRoleStr(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_getStatusStr(t *testing.T) {
	tests := []struct {
		value    float64
		expected string
	}{
		{-0.4, "offline"},
		{0, "offline"},
		{0.4, "offline"},
		{0.5, "ready"},
		{1, "ready"},
		{1.4, "ready"},
		{1.5, "unknown"},
		{2, "unknown"},
		{-0.5, "unknown"},
		{-1, "unknown"},
	}

	for _, tt := range tests {
		res := getStatusStr(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}
