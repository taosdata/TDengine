package tools

import (
	"reflect"
	"testing"
)

// @author: xftan
// @date: 2021/12/14 15:17
// @description: test aes encrypt
func TestAesEncrypt(t *testing.T) {
	type args struct {
		origData []byte
		key      []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				origData: []byte("root:taosdata"),
				key:      []byte("aaaaaaaaaaaaaaaa"),
			},
			want:    []byte{15, 158, 99, 183, 21, 212, 41, 44, 145, 61, 64, 133, 207, 110, 220, 70},
			wantErr: false,
		},
		{
			name: "wrong key",
			args: args{
				origData: []byte("root:taosdata"),
				key:      []byte("a"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AesEncrypt(tt.args.origData, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("AesEncrypt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AesEncrypt() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:17
// @description: test aes decrypt
func TestAesDecrypt(t *testing.T) {
	type args struct {
		encrypted []byte
		key       []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				encrypted: []byte{15, 158, 99, 183, 21, 212, 41, 44, 145, 61, 64, 133, 207, 110, 220, 70},
				key:       []byte("aaaaaaaaaaaaaaaa"),
			},
			want:    []byte("root:taosdata"),
			wantErr: false,
		},
		{
			name: "wrong key",
			args: args{
				encrypted: []byte{15, 158, 99, 183, 21, 212, 41, 44, 145, 61, 64, 133, 207, 110, 220, 70},
				key:       []byte("a"),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AesDecrypt(tt.args.encrypted, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("AesDecrypt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AesDecrypt() got = %v, want %v", got, tt.want)
			}
		})
	}
}
