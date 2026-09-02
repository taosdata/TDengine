package tool

import "strconv"

func ConvertBytesToInt8(bs []byte) (int8, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 8)
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

func ConvertBytesToInt16(bs []byte) (int16, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(v), nil
}

func ConvertBytesToInt32(bs []byte) (int32, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func ConvertBytesToInt64(bs []byte) (int64, error) {
	v, err := strconv.ParseInt(BytesToString(bs), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func BoolToInt8(b bool) int8 {
	if b {
		return 1
	}
	return 0
}
