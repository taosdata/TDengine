package tool

import (
	"testing"
	"time"
)

func TestCharHelpers(t *testing.T) {
	if !IsLetter('a') || !IsLetter('Z') || !IsLetter('_') {
		t.Fatalf("expected letter checks to pass")
	}
	if IsLetter('1') {
		t.Fatalf("digit should not be treated as letter")
	}

	if !IsDigit('0') || !IsDigit('9') {
		t.Fatalf("expected digit checks to pass")
	}
	if IsDigit('a') {
		t.Fatalf("letter should not be treated as digit")
	}

	if DigitVal('0') != 0 || DigitVal('9') != 9 {
		t.Fatalf("unexpected decimal digit values")
	}
	if DigitVal('a') != 10 || DigitVal('f') != 15 {
		t.Fatalf("unexpected lowercase hex digit values")
	}
	if DigitVal('A') != 10 || DigitVal('F') != 15 {
		t.Fatalf("unexpected uppercase hex digit values")
	}
	if DigitVal('x') != 16 {
		t.Fatalf("invalid hex digit should return 16")
	}

	if !IsCarat('.') || !IsCarat('\'') || !IsCarat('"') || !IsCarat('`') {
		t.Fatalf("expected carat characters to match")
	}
	if IsCarat('x') {
		t.Fatalf("unexpected carat character match")
	}
}

func TestDurationConstructors(t *testing.T) {
	d := NewTDDuration(2 * time.Second)
	if d.FixedDuration != 2*time.Second || d.Month != 0 {
		t.Fatalf("unexpected fixed duration struct: %+v", d)
	}

	m := NewTDDurationWithMonth(3)
	if m.FixedDuration != 0 || m.Month != 3 {
		t.Fatalf("unexpected month duration struct: %+v", m)
	}
}

func TestParseDuration_AllUnitsAndErrors(t *testing.T) {
	cases := []struct {
		in         string
		wantFixed  time.Duration
		wantMonth  int64
		expectFail bool
	}{
		{in: "1b", wantFixed: time.Nanosecond},
		{in: "2u", wantFixed: 2 * time.Microsecond},
		{in: "3a", wantFixed: 3 * time.Millisecond},
		{in: "4s", wantFixed: 4 * time.Second},
		{in: "5m", wantFixed: 5 * time.Minute},
		{in: "6h", wantFixed: 6 * time.Hour},
		{in: "7d", wantFixed: 7 * 24 * time.Hour},
		{in: "8w", wantFixed: 8 * 7 * 24 * time.Hour},
		{in: "9n", wantMonth: 9},
		{in: "2y", wantMonth: 24},
		{in: "1S", wantFixed: time.Second},
		{in: "1Q", expectFail: true},
		{in: "xs", expectFail: true},
	}

	for _, tc := range cases {
		got, err := ParseDuration([]byte(tc.in))
		if tc.expectFail {
			if err == nil {
				t.Fatalf("expected parse failure for %q", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected parse failure for %q: %v", tc.in, err)
		}
		if got.FixedDuration != tc.wantFixed || got.Month != tc.wantMonth {
			t.Fatalf("unexpected parse result for %q: got=%+v", tc.in, got)
		}
	}
}

func TestBytesStringHelpers(t *testing.T) {
	raw := []byte("hello")
	if got := BytesToString(raw); got != "hello" {
		t.Fatalf("unexpected BytesToString result: %q", got)
	}

	s := "world"
	bs := StringToBytes(s)
	if string(bs) != s {
		t.Fatalf("unexpected StringToBytes result: %q", string(bs))
	}
}

func TestHasDurationUnit(t *testing.T) {
	valid := []string{"1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w", "1n", "1y", "1S", "1Y"}
	for _, v := range valid {
		if !HasDurationUnit([]byte(v)) {
			t.Fatalf("expected duration unit for %q", v)
		}
	}
	if HasDurationUnit([]byte("1q")) {
		t.Fatalf("unexpected duration unit match for 1q")
	}
}
