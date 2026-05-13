package common

import (
	"math/big"
	"strings"
)

func FormatI128(hi int64, lo uint64) string {
	num := new(big.Int).SetInt64(hi)
	num.Lsh(num, 64)
	num.Or(num, new(big.Int).SetUint64(lo))
	return num.String()
}

func FormatDecimal(str string, scale int) string {
	if scale == 0 {
		return str
	}
	builder := strings.Builder{}
	if strings.HasPrefix(str, "-") {
		str = str[1:]
		builder.WriteByte('-')
	}

	delta := len(str) - scale
	if delta > 0 {
		builder.Grow(len(str) + 1)
		builder.WriteString(str[:delta])
		builder.WriteString(".")
		builder.WriteString(str[delta:])
		return builder.String()
	}
	delta = -delta
	builder.Grow(len(str) + 2 + delta)
	builder.WriteString("0.")
	for i := 0; i < delta; i++ {
		builder.WriteString("0")
	}
	builder.WriteString(str)
	return builder.String()
}
