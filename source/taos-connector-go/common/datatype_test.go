package common

import "testing"

func TestGetTypeName(t *testing.T) {
	type args struct {
		dataType int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "invalid data type",
			args: args{
				dataType: -1,
			},
			want: "",
		},
		{
			name: "over max data type",
			args: args{
				dataType: TSDB_DATA_TYPE_MAX,
			},
			want: "",
		},
		{
			name: "valid data type",
			args: args{
				dataType: TSDB_DATA_TYPE_BINARY,
			},
			want: "VARCHAR",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTypeName(tt.args.dataType); got != tt.want {
				t.Errorf("GetTypeName() = %v, want %v", got, tt.want)
			}
		})
	}
}
