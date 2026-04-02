package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatI128(t *testing.T) {
	type args struct {
		hi int64
		lo uint64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "-1234",
			args: args{
				hi: -1,
				lo: 18446744073709550382,
			},
			want: "-1234",
		},
		{
			name: "0",
			args: args{
				hi: 0,
				lo: 0,
			},
			want: "0",
		},
		{
			name: "1234",
			args: args{
				hi: 0,
				lo: 1234,
			},
			want: "1234",
		},
		{
			name: "max_int64",
			args: args{
				hi: 0,
				lo: 9223372036854775807,
			},
			want: "9223372036854775807",
		},
		{
			name: "min_int64",
			args: args{
				hi: -1,
				lo: 9223372036854775808,
			},
			want: "-9223372036854775808",
		},
		{
			name: "max_uint64",
			args: args{
				hi: 0,
				lo: 18446744073709551615,
			},
			want: "18446744073709551615",
		},
		{
			name: "max_int128",
			args: args{
				hi: 9223372036854775807,
				lo: 18446744073709551615,
			},
			want: "170141183460469231731687303715884105727",
		},
		{
			name: "min_int128",
			args: args{
				hi: -9223372036854775808,
				lo: 0,
			},
			want: "-170141183460469231731687303715884105728",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, FormatI128(tt.args.hi, tt.args.lo), "FormatI128(%v, %v)", tt.args.hi, tt.args.lo)
		})
	}
}

func TestFormatDecimal(t *testing.T) {
	type args struct {
		str   string
		scale int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "0",
			args: args{
				str:   "0",
				scale: 0,
			},
			want: "0",
		},
		{
			name: "0.0",
			args: args{
				str:   "0",
				scale: 1,
			},
			want: "0.0",
		},
		{
			name: "0.00",
			args: args{
				str:   "0",
				scale: 2,
			},
			want: "0.00",
		},
		{
			name: "1",
			args: args{
				str:   "1",
				scale: 0,
			},
			want: "1",
		},
		{
			name: "0.1",
			args: args{
				str:   "1",
				scale: 1,
			},
			want: "0.1",
		},
		{
			name: "-1",
			args: args{
				str:   "-1",
				scale: 0,
			},
			want: "-1",
		},
		{
			name: "-0.1",
			args: args{
				str:   "-1",
				scale: 1,
			},
			want: "-0.1",
		},
		{
			name: "170141183460469231731687303715884.105727",
			args: args{
				str:   "170141183460469231731687303715884105727",
				scale: 6,
			},
			want: "170141183460469231731687303715884.105727",
		},
		{
			name: "-170141183460469231731687303715884.105728",
			args: args{
				str:   "-170141183460469231731687303715884105728",
				scale: 6,
			},
			want: "-170141183460469231731687303715884.105728",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, FormatDecimal(tt.args.str, tt.args.scale), "FormatDecimal(%v, %v)", tt.args.str, tt.args.scale)
		})
	}
}
