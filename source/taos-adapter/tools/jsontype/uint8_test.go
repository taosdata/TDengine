package jsontype

import (
	"reflect"
	"testing"
)

func TestJsonUint8_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		m       JsonUint8
		want    []byte
		wantErr bool
	}{
		{
			name:    "null",
			m:       nil,
			want:    []byte("null"),
			wantErr: false,
		},
		{
			name:    "common",
			m:       JsonUint8{1, 2, 3, 4, 5},
			want:    []byte("[1,2,3,4,5]"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.m.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}
