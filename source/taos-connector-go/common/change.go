package common

import (
	"fmt"
	"strings"
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

func TimestampConvertToTimeWithLocation(timestamp int64, precision int, loc *time.Location) time.Time {
	switch precision {
	case PrecisionMilliSecond: // milli-second
		return time.Unix(0, timestamp*1e6).In(loc)
	case PrecisionMicroSecond: // micro-second
		return time.Unix(0, timestamp*1e3).In(loc)
	case PrecisionNanoSecond: // nano-second
		return time.Unix(0, timestamp).In(loc)
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

func ParseTimezone(tz string) (*time.Location, error) {
	if tz == "" {
		return nil, fmt.Errorf("empty string")
	}
	if strings.ToLower(tz) == "local" {
		return nil, fmt.Errorf("timezone cannot be 'Local'")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	return loc, nil
}
