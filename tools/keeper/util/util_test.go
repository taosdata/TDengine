// Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "abc", want: "abc"},
		{name: "single quote", in: "a'b", want: "a''b"},
		{name: "backslash", in: `a\b`, want: `a\\b`},
		{name: "backslash quote", in: `a\'b`, want: `a\\''b`},
		{name: "injection attempt", in: "x', 0) values (now) y tags ('z", want: "x'', 0) values (now) y tags (''z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EscapeSQLString(tt.in))
		})
	}
}

func TestEscapeTrunc(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		width int
		want  string
	}{
		{name: "plain ascii passthrough", in: "abc", width: 10, want: "abc"},
		{name: "empty string", in: "", width: 10, want: ""},
		{name: "truncate at width", in: "abcdef", width: 3, want: "abc"},
		{name: "quote escaped after truncation", in: "ab'cd", width: 5, want: "ab''cd"},
		{name: "backslash escaped after truncation", in: `ab\cd`, width: 5, want: `ab\\cd`},
		// width 3 cuts to "ab'", which then escapes to 4 bytes: the escaped
		// literal may exceed width, TDengine checks width unescaped
		{name: "escaped literal may exceed width", in: "ab'cd", width: 3, want: "ab''"},
		// width caps bytes but never splits a rune: 7 bytes lands inside 试,
		// so the cut retreats to the 6-byte boundary
		{name: "cjk truncation retreats off mid-rune cut", in: "中文测试", width: 7, want: "中文"},
		{name: "cjk truncation mid second rune", in: "中文测试", width: 4, want: "中"},
		{name: "cjk truncation on rune boundary", in: "中文测试", width: 6, want: "中文"},
		{name: "cjk quote at truncation boundary", in: "中'文", width: 4, want: "中''"},
		{name: "cjk quote then mid-rune cut", in: "中'文", width: 5, want: "中''"},
		{name: "zero width", in: "abc", width: 0, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EscapeTrunc(tt.in, tt.width))
		})
	}
}

func TestSafeSubstringUTF8(t *testing.T) {
	// never returns invalid UTF-8, unlike SafeSubstring which cuts by bytes
	assert.Equal(t, "中文", SafeSubstringUTF8("中文测试", 7))
	assert.Equal(t, "中", SafeSubstringUTF8("中文", 4))
	assert.Equal(t, "中文", SafeSubstringUTF8("中文", 6))
	assert.Equal(t, "中文", SafeSubstringUTF8("中文", 100))
	assert.Equal(t, "abc", SafeSubstringUTF8("abcdef", 3))
	assert.Equal(t, "", SafeSubstringUTF8("中文", 1))
}

func TestEscapeInfluxProtocolPreservesTagValueBytes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "delimiters", in: "a,b=c d", want: `a\,b\=c\ d`},
		{name: "quote", in: `a"b`, want: `a"b`},
		{name: "trailing backslash", in: `tail\`, want: `tail\\`},
		{name: "backslash next to delimiters", in: `a\,b\=c\ d`, want: `a\\\,b\\\=c\\\ d`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EscapeInfluxProtocol(tt.in))
		})
	}
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

func TestSafeSubstringUTF8NonPositiveWidth(t *testing.T) {
	assert.Equal(t, "", SafeSubstringUTF8("abc", 0))
	assert.Equal(t, "", SafeSubstringUTF8("abc", -1))
	assert.Equal(t, "", SafeSubstringUTF8("", -5))
}
