// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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
	"crypto/md5"
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// https://github.com/containerd/cgroups/blob/main/utils.go
var globalCounter64 uint64
var globalCounter32 uint32

var MAX_TABLE_NAME_LEN = 190

func init() {
	atomic.StoreUint64(&globalCounter64, 0)
	atomic.StoreUint32(&globalCounter32, 0)
}

func ReadUint(path string) (uint64, error) {
	v, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return ParseUint(strings.TrimSpace(string(v)), 10, 64)
}

func ParseUint(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		// 1. Handle negative values greater than MinInt64 (and)
		// 2. Handle negative values lesser than MinInt64
		if intErr == nil && intValue < 0 {
			return 0, nil
		} else if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// SafeSubstringRunes truncates s to at most n characters (runes). Use it for
// nchar(N) columns: TDengine sizes those in characters, so a value of N
// characters fits however many bytes it occupies. Truncating to N bytes instead
// would cut multi-byte text to a fraction of the column, while letting an
// over-long ASCII value through to fail the whole insert with "Value too long".
func SafeSubstringRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	// the loop falls through when s is short enough, so it needs no separate
	// rune count up front
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}

// EscapeTruncRunes truncates s to at most n characters and then escapes the
// result for a single-quoted SQL string literal (like EscapeSQLString).
func EscapeTruncRunes(s string, n int) string {
	return EscapeSQLString(SafeSubstringRunes(s, n))
}

func EscapeInfluxProtocol(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, " ", "\\ ")
	return s
}

func SafeSubstring(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// EscapeSQLString escapes a value for a single-quoted SQL string literal:
// backslashes are doubled, and single quotes are doubled so they cannot
// terminate the literal.
func EscapeSQLString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

// EscapeTrunc truncates s to at most width bytes without splitting a
// multi-byte UTF-8 character (the cut retreats to the last rune boundary),
// then escapes the result for a single-quoted SQL string literal (like
// EscapeSQLString). Use it everywhere a request-controlled string is
// concatenated into SQL so truncation and escaping never drift apart.
// TDengine checks a varchar width against the unescaped value, so the
// escaped literal may exceed width bytes.
func EscapeTrunc(s string, width int) string {
	return EscapeSQLString(SafeSubstringUTF8(s, width))
}

// SafeSubstringUTF8 truncates s to at most n bytes, backing off to the last
// rune boundary so the result is always valid UTF-8. Unlike SafeSubstring it
// never emits a split multi-byte character.
func SafeSubstringUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	truncated := s[:n]
	for !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

func GetQid(qidStr string) uint64 {
	if qidStr == "" || !strings.HasPrefix(qidStr, "0x") {
		qid32 := atomic.AddUint32(&globalCounter32, 1)
		qid64 := uint64(qid32) << 8
		return qid64
	}

	qid, err := strconv.ParseUint(qidStr[2:], 16, 64)
	if err != nil {
		qid32 := atomic.AddUint32(&globalCounter32, 1)
		qid64 := uint64(qid32) << 8
		return qid64
	}

	// clear the last byte
	qid = qid &^ 0xFF

	return qid
}

func GetQidOwn(instanceId uint8) uint64 {
	id := atomic.AddUint64(&globalCounter64, 1)
	if id > 0x00ffffffffffffff {
		atomic.StoreUint64(&globalCounter64, 1)
		id = 1
	}
	return uint64(instanceId)<<56 | id
}

func GetMd5HexStr(str string) string {
	sum := md5.Sum([]byte(str))
	return hex.EncodeToString(sum[:])
}

func isValidChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// ToValidTableName maps input onto an unquoted TDengine identifier: every rune
// that is not a letter, a digit or '_' becomes '_', and the result is lowercased.
//
// This is Unicode-aware, not an ASCII [A-Za-z0-9_] filter — "汉字" and "ABC" are
// letters and survive. A non-ASCII name therefore still needs backticks in
// TDengine (an unquoted one is a syntax error), and an uppercase ASCII name is
// folded, so two inputs can map to the same identifier. Callers that need an
// ASCII-only or collision-free name must constrain the input themselves;
// processString in api/gen_metric.go is the ASCII-only variant.
func ToValidTableName(input string) string {
	var builder strings.Builder
	for _, r := range input {
		if isValidChar(r) {
			builder.WriteRune(unicode.ToLower(r))
		} else {
			builder.WriteRune('_')
		}
	}
	return builder.String()
}

func HandleIp(host string) string {
	if host == "" {
		return host
	}

	ipPart := host
	if idx := strings.Index(host, "%"); idx != -1 {
		ipPart = host[:idx]
	}

	ip := net.ParseIP(ipPart)
	if ip == nil {
		return host
	}

	if ip.To4() == nil {
		return "[" + host + "]"
	}

	return host
}
