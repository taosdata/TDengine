package inputjson

import (
	"testing"
	"time"

	"github.com/ncruces/go-strftime"
	"github.com/stretchr/testify/assert"
)

func TestIsPredefinedTimeFormat(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   bool
	}{
		{"unix format", "unix", true},
		{"unix_ms format", "unix_ms", true},
		{"unix_us format", "unix_us", true},
		{"unix_ns format", "unix_ns", true},
		{"ansic format", "ansic", true},
		{"rubydate format", "rubydate", true},
		{"rfc822z format", "rfc822z", true},
		{"rfc1123z format", "rfc1123z", true},
		{"rfc3339 format", "rfc3339", true},
		{"rfc3339nano format", "rfc3339nano", true},
		{"stamp format", "stamp", true},
		{"stampmilli format", "stampmilli", true},
		{"datetime format", "datetime", true},
		{"custom format", "2006-01-02", false},
		{"empty format", "", false},
		{"unknown format", "unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPredefinedTimeFormat(tt.format); got != tt.want {
				t.Errorf("isPredefinedTimeFormat(%q) = %v, want %v", tt.format, got, tt.want)
			}
		})
	}
}

func TestParseTime_UnixFormats(t *testing.T) {
	loc := time.UTC
	tests := []struct {
		name    string
		strTime string
		format  string
		want    time.Time
		wantErr bool
	}{
		{
			name:    "unix timestamp seconds",
			strTime: "1696516245",
			format:  "unix",
			want:    time.Unix(1696516245, 0),
			wantErr: false,
		},
		{
			name:    "unix_ms timestamp milliseconds",
			strTime: "1696516245123",
			format:  "unix_ms",
			want:    time.UnixMilli(1696516245123),
			wantErr: false,
		},
		{
			name:    "unix_us timestamp microseconds",
			strTime: "1696516245123456",
			format:  "unix_us",
			want:    time.UnixMicro(1696516245123456),
			wantErr: false,
		},
		{
			name:    "unix_ns timestamp nanoseconds",
			strTime: "1696516245123456789",
			format:  "unix_ns",
			want:    time.Unix(0, 1696516245123456789),
			wantErr: false,
		},
		{
			name:    "invalid unix timestamp",
			strTime: "invalid",
			format:  "unix",
			want:    time.Time{},
			wantErr: true,
		},
		{
			name:    "invalid unix_ms timestamp",
			strTime: "invalid",
			format:  "unix_ms",
			want:    time.Time{},
			wantErr: true,
		},
		{
			name:    "invalid unix_us timestamp",
			strTime: "invalid",
			format:  "unix_us",
			want:    time.Time{},
			wantErr: true,
		},
		{
			name:    "invalid unix_ns timestamp",
			strTime: "invalid",
			format:  "unix_ns",
			want:    time.Time{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTime(tt.strTime, tt.format, loc)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("parseTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTime_StringFormats(t *testing.T) {
	loc := time.UTC

	tests := []struct {
		name    string
		strTime string
		format  string
		want    time.Time
		wantErr bool
	}{
		{
			name:    "ANSIC format",
			strTime: "Thu Oct  5 12:30:45 2025",
			format:  "ansic",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 0, loc),
			wantErr: false,
		},
		{
			name:    "RubyDate format",
			strTime: "Thu Oct 05 12:30:45 +0000 2025",
			format:  "rubydate",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 0, loc),
			wantErr: false,
		},
		{
			name:    "RFC822Z format",
			strTime: "05 Oct 25 12:30 +0000",
			format:  "rfc822z",
			want:    time.Date(2025, 10, 5, 12, 30, 0, 0, loc),
			wantErr: false,
		},
		{
			name:    "RFC1123Z format",
			strTime: "Thu, 05 Oct 2025 12:30:45 +0000",
			format:  "rfc1123z",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 0, loc),
			wantErr: false,
		},
		{
			name:    "RFC3339 format",
			strTime: "2025-10-05T12:30:45Z",
			format:  "rfc3339",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 0, loc),
			wantErr: false,
		},
		{
			name:    "RFC3339Nano format",
			strTime: "2025-10-05T12:30:45.123456789Z",
			format:  "rfc3339nano",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 123456789, loc),
			wantErr: false,
		},
		{
			name:    "Stamp format",
			strTime: "Oct  5 12:30:45",
			format:  "stamp",
			want:    time.Date(0, 10, 5, 12, 30, 45, 0, loc),
			wantErr: false,
		},
		{
			name:    "StampMilli format",
			strTime: "Oct  5 12:30:45.123",
			format:  "stampmilli",
			want:    time.Date(0, 10, 5, 12, 30, 45, 123000000, loc),
			wantErr: false,
		},
		{
			name:    "datetime format",
			strTime: "2025-10-05 12:30:45.123456789",
			format:  "datetime",
			want:    time.Date(2025, 10, 5, 12, 30, 45, 123456789, loc),
			wantErr: false,
		},
		{
			name:    "invalid time string",
			strTime: "invalid time",
			format:  "ansic",
			want:    time.Time{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTime(tt.strTime, tt.format, loc)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("parseTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTime_Location(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	shanghaiLoc, _ := time.LoadLocation("Asia/Shanghai")
	londonLoc, _ := time.LoadLocation("Europe/London")

	tests := []struct {
		name          string
		strTime       string
		format        string
		loc           *time.Location
		wantTimestamp int64
		wantErr       bool
	}{
		{
			name:          "UTC location",
			strTime:       "2025-10-05 12:30:45",
			format:        "datetime",
			loc:           time.UTC,
			wantTimestamp: 1759667445,
			wantErr:       false,
		},
		{
			// London is UTC+1 in October due to DST
			name:          "London location",
			strTime:       "2025-10-05 12:30:45",
			format:        "datetime",
			loc:           londonLoc,
			wantTimestamp: 1759663845,
			wantErr:       false,
		},
		{
			name:          "New York location",
			strTime:       "2025-10-05 12:30:45",
			format:        "datetime",
			loc:           nyLoc,
			wantTimestamp: 1759681845,
			wantErr:       false,
		},
		{
			name:          "Shanghai location",
			strTime:       "2025-10-05 12:30:45",
			format:        "datetime",
			loc:           shanghaiLoc,
			wantTimestamp: 1759638645,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTime(tt.strTime, tt.format, tt.loc)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got.Location() != tt.loc {
				t.Errorf("parseTime() loc = %v, want %v", got.Location(), tt.loc)
			}
			if !tt.wantErr && got.Unix() != tt.wantTimestamp {
				t.Errorf("parseTime() timestamp = %v, want %v", got.Unix(), tt.wantTimestamp)
			}
		})
	}
}

func TestParseTime_EdgeCases(t *testing.T) {
	loc := time.UTC

	tests := []struct {
		name    string
		strTime string
		format  string
		wantErr bool
	}{
		{
			name:    "empty time string",
			strTime: "",
			format:  "unix",
			wantErr: true,
		},
		{
			name:    "empty format",
			strTime: "2023-10-05",
			format:  "",
			wantErr: true,
		},
		{
			name:    "negative unix timestamp",
			strTime: "-1000",
			format:  "unix",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseTime(tt.strTime, tt.format, loc)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTime() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseTime_Strftime(t *testing.T) {
	timeFormat := "%Y-%m-%d %H:%M:%S"
	format, err := strftime.Layout(timeFormat)
	assert.NoError(t, err)
	assert.Equal(t, "2006-01-02 15:04:05", format)
	strTime := "2025-10-05 12:30:45"
	loc := time.UTC
	want := time.Date(2025, 10, 5, 12, 30, 45, 0, loc)

	got, err := parseTime(strTime, format, loc)
	if err != nil {
		t.Errorf("parseTime() error = %v, wantErr %v", err, false)
		return
	}
	if !got.Equal(want) {
		t.Errorf("parseTime() = %v, want %v", got, want)
	}
}
