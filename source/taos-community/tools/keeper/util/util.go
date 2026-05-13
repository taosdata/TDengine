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

func EscapeInfluxProtocol(s string) string {
	s = strings.TrimSuffix(s, "\\")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

func SafeSubstring(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
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
