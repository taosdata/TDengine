package util

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandleIp(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{
			name: "IPv4 address",
			host: "192.168.1.1",
			want: "192.168.1.1",
		},
		{
			name: "IPv6 address",
			host: "2001:db8::1",
			want: "[2001:db8::1]",
		},
		{
			name: "Link-local without scope",
			host: "fe80::a6bb:6dff:fed9:9817",
			want: "[fe80::a6bb:6dff:fed9:9817]",
		},
		{
			name: "Link-local with scope",
			host: "fe80::a6bb:6dff:fed9:9817%eno1",
			want: "[fe80::a6bb:6dff:fed9:9817%eno1]",
		},
		{
			name: "Hostname",
			host: "example.com",
			want: "example.com",
		},
		{
			name: "Invalid IP",
			host: "256.256.256.2567",
			want: "256.256.256.2567",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HandleIp(tc.host)
			if got != tc.want {
				t.Errorf("HandleIp(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}

func TestReadUint_ReadFileError_ReturnsZeroAndError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent")
	v, err := ReadUint(path)
	assert.Error(t, err)
	assert.Equal(t, uint64(0), v)
}

func TestParseUint_NegativeWithinRange_ReturnsZeroNil(t *testing.T) {
	v, err := ParseUint("-1", 10, 64)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), v)
}

func TestParseUint_NegativeOverflow_ReturnsZeroNil(t *testing.T) {
	v, err := ParseUint("-9223372036854775809", 10, 64)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), v)
}

func TestSafeSubstring(t *testing.T) {
	res := SafeSubstring("hello world", 5)
	assert.Equal(t, "hello", res)
}

func TestGetQidOwn_CounterWraps_ResetsToOne(t *testing.T) {
	const boundary = 0x00ffffffffffffff

	atomic.StoreUint64(&globalCounter64, boundary)

	inst := uint8(0xAB)
	got := GetQidOwn(inst)

	if got>>56 != uint64(inst) {
		t.Fatalf("instance id mismatch: got 0x%x, want 0x%x", got>>56, inst)
	}
	if (got & boundary) != 1 {
		t.Fatalf("low 56 bits = %d, want 1", got&boundary)
	}
	if atomic.LoadUint64(&globalCounter64) != 1 {
		t.Fatalf("globalCounter64 = %d, want 1", atomic.LoadUint64(&globalCounter64))
	}
}

func TestGetQid(t *testing.T) {
	atomic.StoreUint32(&globalCounter32, 0)
	qid := GetQid("0xzxx")
	if qid != 1<<8 {
		t.Fatalf("qid=%d, want %d", qid, 1<<8)
	}
}
