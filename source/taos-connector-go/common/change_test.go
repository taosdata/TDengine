package common

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// @author: xftan
// @date: 2022/1/25 16:55
// @description: test timestamp with precision convert to time.Time
func TestTimestampConvertToTime(t *testing.T) {
	type args struct {
		timestamp int64
		precision int
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "ms",
			args: args{
				timestamp: 1643068800000,
				precision: PrecisionMilliSecond,
			},
			want: time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "us",
			args: args{
				timestamp: 1643068800000000,
				precision: PrecisionMicroSecond,
			},
			want: time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "ns",
			args: args{
				timestamp: 1643068800000000000,
				precision: PrecisionNanoSecond,
			},
			want: time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimestampConvertToTime(tt.args.timestamp, tt.args.precision); !reflect.DeepEqual(got.UTC(), tt.want.UTC()) {
				t.Errorf("TimestampConvertToTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/25 16:56
// @description: test time.Time with precision convert to timestamp
func TestTimeToTimestamp(t *testing.T) {
	type args struct {
		t         time.Time
		precision int
	}
	tests := []struct {
		name          string
		args          args
		wantTimestamp int64
	}{
		{
			name: "ms",
			args: args{
				t:         time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
				precision: PrecisionMilliSecond,
			},
			wantTimestamp: 1643068800000,
		},
		{
			name: "us",
			args: args{
				t:         time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
				precision: PrecisionMicroSecond,
			},
			wantTimestamp: 1643068800000000,
		},
		{
			name: "ns",
			args: args{
				t:         time.Date(2022, 01, 25, 0, 0, 0, 0, time.UTC),
				precision: PrecisionNanoSecond,
			},
			wantTimestamp: 1643068800000000000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotTimestamp := TimeToTimestamp(tt.args.t, tt.args.precision); gotTimestamp != tt.wantTimestamp {
				t.Errorf("TimeToTimestamp() = %v, want %v", gotTimestamp, tt.wantTimestamp)
			}
		})
	}
}

func TestTimestampConvertToTimeWithLocation(t *testing.T) {
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	type args struct {
		timestamp int64
		precision int
		loc       *time.Location
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "ms",
			args: args{
				timestamp: 1643068800000,
				precision: PrecisionMilliSecond,
				loc:       parisTimezone,
			},
			want: time.Date(2022, 01, 25, 1, 0, 0, 0, parisTimezone),
		},
		{
			name: "us",
			args: args{
				timestamp: 1643068800000000,
				precision: PrecisionMicroSecond,
				loc:       parisTimezone,
			},
			want: time.Date(2022, 01, 25, 1, 0, 0, 0, parisTimezone),
		},
		{
			name: "ns",
			args: args{
				timestamp: 1643068800000000000,
				precision: PrecisionNanoSecond,
				loc:       parisTimezone,
			},
			want: time.Date(2022, 01, 25, 1, 0, 0, 0, parisTimezone),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, TimestampConvertToTimeWithLocation(tt.args.timestamp, tt.args.precision, tt.args.loc), "TimestampConvertToTimeWithLocation(%v, %v, %v)", tt.args.timestamp, tt.args.precision, tt.args.loc)
		})
	}
}

func TestTimestampConvertTotimeWithLocationPanic(t *testing.T) {
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	assert.Panics(t, func() {
		TimestampConvertToTimeWithLocation(1643068800000, 3, parisTimezone)
	}, "TimestampConvertToTimeWithLocation should panic")
}
