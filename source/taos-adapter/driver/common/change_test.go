package common

import (
	"reflect"
	"testing"
	"time"
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
