package common

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/25 17:47
// @description: test sql interpolate params
func TestInterpolateParams(t *testing.T) {
	type args struct {
		query string
		args  []driver.NamedValue
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "all type",
			args: args{
				query: "select * from t1 where " +
					"ts = ? and " +
					"i8 = ? and " +
					"i16 = ? and " +
					"i32 = ? and " +
					"i64 = ? and " +
					"ui8 = ? and " +
					"ui16 = ? and " +
					"ui32 = ? and " +
					"ui64 = ? and " +
					"f32 = ? and " +
					"f64 = ? and " +
					"i = ? and " +
					"u = ? and " +
					"b = ? and " +
					"bs = ? and " +
					"str = ? and " +
					"nil is ?",
				args: []driver.NamedValue{
					{Ordinal: 1, Value: time.Unix(1643068800, 0).UTC()},
					{Ordinal: 2, Value: int8(1)},
					{Ordinal: 3, Value: int16(2)},
					{Ordinal: 4, Value: int32(3)},
					{Ordinal: 5, Value: int64(4)},
					{Ordinal: 6, Value: uint8(1)},
					{Ordinal: 7, Value: uint16(2)},
					{Ordinal: 8, Value: uint32(3)},
					{Ordinal: 9, Value: uint64(4)},
					{Ordinal: 10, Value: float32(5.2)},
					{Ordinal: 11, Value: float64(5.2)},
					{Ordinal: 12, Value: 6},
					{Ordinal: 13, Value: uint(6)},
					{Ordinal: 14, Value: true},
					{Ordinal: 15, Value: []byte("'bytes'")},
					{Ordinal: 16, Value: []byte("'str'")},
					{Ordinal: 17, Value: nil},
				},
			},
			want: "select * from t1 where " +
				"ts = '2022-01-25T00:00:00Z' and " +
				"i8 = 1 and " +
				"i16 = 2 and " +
				"i32 = 3 and " +
				"i64 = 4 and " +
				"ui8 = 1 and " +
				"ui16 = 2 and " +
				"ui32 = 3 and " +
				"ui64 = 4 and " +
				"f32 = 5.200000 and " +
				"f64 = 5.200000 and " +
				"i = 6 and " +
				"u = 6 and " +
				"b = 1 and " +
				"bs = 'bytes' and " +
				"str = 'str' and " +
				"nil is NULL",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InterpolateParams(tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("InterpolateParams() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("InterpolateParams() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValueArgsToNamedValueArgs(t *testing.T) {
	tests := []struct {
		name string
		args []driver.Value
		want []driver.NamedValue
	}{
		{
			name: "empty args",
			args: []driver.Value{},
			want: []driver.NamedValue{},
		},
		{
			name: "single arg",
			args: []driver.Value{int64(1)},
			want: []driver.NamedValue{
				{Ordinal: 1, Value: int64(1)},
			},
		},
		{
			name: "multiple args",
			args: []driver.Value{int64(1), "test", nil},
			want: []driver.NamedValue{
				{Ordinal: 1, Value: int64(1)},
				{Ordinal: 2, Value: "test"},
				{Ordinal: 3, Value: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValueArgsToNamedValueArgs(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ValueArgsToNamedValueArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}
