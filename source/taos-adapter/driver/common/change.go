package common

import (
	"fmt"
	"time"
)

func TimestampConvertToTime(timestamp int64, precision int) time.Time {
	switch precision {
	case PrecisionMilliSecond: // milli-second
		return time.Unix(0, timestamp*1e6)
	case PrecisionMicroSecond: // micro-second
		return time.Unix(0, timestamp*1e3)
	case PrecisionNanoSecond: // nano-second
		return time.Unix(0, timestamp)
	default:
		s := fmt.Sprintln("unknown precision", precision, "timestamp", timestamp)
		panic(s)
	}
}

func TimeToTimestamp(t time.Time, precision int) (timestamp int64) {
	switch precision {
	case PrecisionMilliSecond:
		return t.UnixNano() / 1e6
	case PrecisionMicroSecond:
		return t.UnixNano() / 1e3
	case PrecisionNanoSecond:
		return t.UnixNano()
	default:
		s := fmt.Sprintln("unknown precision", precision, "time", t)
		panic(s)
	}
}
