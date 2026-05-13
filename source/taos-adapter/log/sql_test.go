package log

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTaosSqlLogFormatter_Format(t1 *testing.T) {
	type args struct {
		entry *logrus.Entry
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "common",
			args: args{
				entry: &logrus.Entry{
					Time:    time.Unix(1657084598, 0),
					Message: "select 1",
				},
			},
			want: []byte(fmt.Sprintf("%s %s select 1\n", time.Unix(1657084598, 0).Format("01/02 15:04:05.000000"), ServerID)),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("%s,%v", err.Error(), i)
					return false
				}
				return true
			},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaosSqlLogFormatter{}
			got, err := t.Format(tt.args.entry)
			if !tt.wantErr(t1, err, fmt.Sprintf("Format(%v)", tt.args.entry)) {
				return
			}
			assert.Equalf(t1, tt.want, got, "Format(%v)", tt.args.entry)
		})
	}
}
