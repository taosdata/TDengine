package regexp

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompile(t *testing.T) {
	reg, err := Compile("^(?!.*_ERROR).*")
	assert.NoError(t, err)
	reg2, err := newRegexp("^(?!.*_ERROR).*")
	assert.NoError(t, err)
	assert.Equal(t, reg2, reg)

	reg1, err := Compile("abc")
	assert.NoError(t, err)
	reg3, err := regexp.Compile("abc")
	assert.NoError(t, err)
	assert.Equal(t, reg3, reg1)

	reg4, err := Compile("?|a|b")
	assert.Error(t, err)
	assert.Nil(t, reg4)
}

func TestMatchString(t *testing.T) {
	type fields struct {
		pattern string
	}
	type args struct {
		s string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "abcdef",
			fields: fields{
				pattern: "^(?!.*_Error).+$",
			},
			args: args{
				s: "abcdef",
			},
			want: true,
		},
		{
			name: "a_Error",
			fields: fields{
				pattern: "^(?!.*_Error).+$",
			},
			args: args{
				s: "a_Error",
			},
			want: false,
		},
		{
			name: "_Errorbb",
			fields: fields{
				pattern: "^(?!.*_Error).+$",
			},
			args: args{
				s: "_Errorbb",
			},
			want: false,
		},
		{
			name: "a._Errorbb",
			fields: fields{
				pattern: "^(?!.*_Error).+$",
			},
			args: args{
				s: "a._Errorbb",
			},
			want: false,
		},
		{
			name: "_Error",
			fields: fields{
				pattern: "^(?!.*_Error).+$",
			},
			args: args{
				s: "_Error",
			},
			want: false,
		},

		{
			name: "abc",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "abc",
			},
			want: true,
		},

		{
			name: "xabc",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "xabc",
			},
			want: true,
		},

		{
			name: "abcy",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "abcy",
			},
			want: true,
		},

		{
			name: "xabcy",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "xabcy",
			},
			want: true,
		},

		{
			name: "ac",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "ac",
			},
			want: false,
		},

		{
			name: "ab",
			fields: fields{
				pattern: "abc",
			},
			args: args{
				s: "ab",
			},
			want: false,
		},

		{
			name: "abcxyz",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "abcxyz",
			},
			want: true,
		},

		{
			name: "1abc2xyz3",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "1abc2xyz3",
			},
			want: true,
		},

		{
			name: "abc2xyz3",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "abc2xyz3",
			},
			want: true,
		},

		{
			name: "abc2xyz",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "abc2xyz",
			},
			want: true,
		},

		{
			name: "abc2yz",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "abc2yz",
			},
			want: false,
		},

		{
			name: "ac2xyz",
			fields: fields{
				pattern: "^(?=.*abc)(?=.*xyz).+$",
			},
			args: args{
				s: "ac2xyz",
			},
			want: false,
		},

		{
			name: "abc_bbd",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "abc_bbd",
			},
			want: true,
		},

		{
			name: "xabc_bbd",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "xabc_bbd",
			},
			want: true,
		},

		{
			name: "xyxabc",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "xyxabc",
			},
			want: true,
		},

		{
			name: "_Errorabc",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "_Errorabc",
			},
			want: false,
		},

		{
			name: "abc_Error",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "abc_Error",
			},
			want: false,
		},

		{
			name: "abc_Errorxyz",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "abc_Errorxyz",
			},
			want: false,
		},

		{
			name: "xyxabcdd_Errorxyz",
			fields: fields{
				pattern: "^(?=.*abc)(?!.*_Error).+$",
			},
			args: args{
				s: "xyxabcdd_Errorxyz",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Compile(tt.fields.pattern)
			assert.NoError(t, err)
			assert.Equalf(t, tt.want, r.MatchString(tt.args.s), "MatchString(%v)", tt.args.s)
		})
	}
}
