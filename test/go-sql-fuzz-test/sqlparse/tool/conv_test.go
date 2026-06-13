package tool

import "testing"

func TestConvertBytesToInt8(t *testing.T) {
	if v, err := ConvertBytesToInt8([]byte("127")); err != nil || v != 127 {
		t.Fatalf("unexpected int8 conversion result: v=%d err=%v", v, err)
	}
	if _, err := ConvertBytesToInt8([]byte("128")); err == nil {
		t.Fatalf("expected overflow error for int8")
	}
	if _, err := ConvertBytesToInt8([]byte("x")); err == nil {
		t.Fatalf("expected parse error for int8")
	}
}

func TestConvertBytesToInt16(t *testing.T) {
	if v, err := ConvertBytesToInt16([]byte("32767")); err != nil || v != 32767 {
		t.Fatalf("unexpected int16 conversion result: v=%d err=%v", v, err)
	}
	if _, err := ConvertBytesToInt16([]byte("32768")); err == nil {
		t.Fatalf("expected overflow error for int16")
	}
	if _, err := ConvertBytesToInt16([]byte("x")); err == nil {
		t.Fatalf("expected parse error for int16")
	}
}

func TestConvertBytesToInt32(t *testing.T) {
	if v, err := ConvertBytesToInt32([]byte("2147483647")); err != nil || v != 2147483647 {
		t.Fatalf("unexpected int32 conversion result: v=%d err=%v", v, err)
	}
	if _, err := ConvertBytesToInt32([]byte("2147483648")); err == nil {
		t.Fatalf("expected overflow error for int32")
	}
	if _, err := ConvertBytesToInt32([]byte("x")); err == nil {
		t.Fatalf("expected parse error for int32")
	}
}

func TestConvertBytesToInt64(t *testing.T) {
	if v, err := ConvertBytesToInt64([]byte("9223372036854775807")); err != nil || v != 9223372036854775807 {
		t.Fatalf("unexpected int64 conversion result: v=%d err=%v", v, err)
	}
	if _, err := ConvertBytesToInt64([]byte("9223372036854775808")); err == nil {
		t.Fatalf("expected overflow error for int64")
	}
	if _, err := ConvertBytesToInt64([]byte("x")); err == nil {
		t.Fatalf("expected parse error for int64")
	}
}

func TestBoolToInt8(t *testing.T) {
	if got := BoolToInt8(true); got != 1 {
		t.Fatalf("expected BoolToInt8(true) == 1, got %d", got)
	}
	if got := BoolToInt8(false); got != 0 {
		t.Fatalf("expected BoolToInt8(false) == 0, got %d", got)
	}
}
