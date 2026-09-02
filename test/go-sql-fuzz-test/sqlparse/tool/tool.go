package tool

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

func IsLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func IsDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}

func DigitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16
}

func IsCarat(ch uint16) bool {
	return ch == '.' || ch == '\'' || ch == '"' || ch == '`'
}

type TDDuration struct {
	FixedDuration time.Duration
	Month         int64
}

func NewTDDuration(d time.Duration) TDDuration {
	return TDDuration{
		FixedDuration: d,
		Month:         0,
	}
}

func NewTDDurationWithMonth(month int64) TDDuration {
	return TDDuration{
		FixedDuration: 0,
		Month:         month,
	}
}

func ParseDuration(s []byte) (TDDuration, error) {
	unit := s[len(s)-1]
	val, err := strconv.ParseInt(BytesToString(s[:len(s)-1]), 10, 64)
	if err != nil {
		return TDDuration{}, fmt.Errorf("invalid duration format: %s", s)
	}
	switch unit {
	case 'b', 'B':
		// nanoseconds
		return NewTDDuration(time.Duration(val) * time.Nanosecond), nil
	case 'u', 'U':
		// microseconds
		return NewTDDuration(time.Duration(val) * time.Microsecond), nil
	case 'a', 'A':
		// milliseconds
		return NewTDDuration(time.Duration(val) * time.Millisecond), nil
	case 's', 'S':
		// seconds
		return NewTDDuration(time.Duration(val) * time.Second), nil
	case 'm', 'M':
		// minutes
		return NewTDDuration(time.Duration(val) * time.Minute), nil
	case 'h', 'H':
		// hours
		return NewTDDuration(time.Duration(val) * time.Hour), nil
	case 'd', 'D':
		// days
		return NewTDDuration(time.Duration(val) * 24 * time.Hour), nil
	case 'w', 'W':
		// weeks
		return NewTDDuration(time.Duration(val) * 7 * 24 * time.Hour), nil
	case 'n', 'N':
		return NewTDDurationWithMonth(val), nil
	case 'y', 'Y':
		return NewTDDurationWithMonth(val * 12), nil
	default:
		return TDDuration{}, fmt.Errorf("invalid duration format: %s", s)
	}
}

func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func HasDurationUnit(s []byte) bool {
	unit := s[len(s)-1]
	switch unit {
	case 'b', 'B', 'u', 'U', 'a', 'A', 's', 'S', 'm', 'M', 'h', 'H', 'd', 'D', 'w', 'W', 'n', 'N', 'y', 'Y':
		return true
	default:
		return false
	}
}
