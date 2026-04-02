package otp

import (
	"reflect"
	"testing"
)

func TestGenerateTOTPSecret(t *testing.T) {
	type args struct {
		seed []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test1",
			args: args{
				seed: []byte("12345678901234567890"),
			},
			want: "VR62SA7EK3RP7MRTH7QXSIVZXXS57OY2SRUMGLKDJPREZ62OHFEQ",
		},
		{
			name: "Test2",
			args: args{
				seed: []byte("abcdefghijklmnopqrstuvwxyz"),
			},
			want: "OMYPD744HIZB2KZPAUNLEWUFBNRQBILEPWD2FGPUDYCZMFTCRFXQ",
		},
		{
			name: "Test3",
			args: args{
				seed: []byte("!@#$%^&*()_+-=[]{}|;':,.<>/?`~"),
			},
			want: "FURKOZ6REIGLQHP5OMKLZZFUQNOCRDJPCOSDP5ESH2VR3IU7NAYA",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TOTPSecretStr(GenerateTOTPSecret(tt.args.seed))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateTOTPSecret() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerateTOTPCode(t *testing.T) {
	type args struct {
		key     []byte
		counter uint64
		digits  int
	}
	ts := uint64(1765854733) / 30
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test1",
			args: args{
				key:     GenerateTOTPSecret([]byte("12345678901234567890")),
				counter: ts,
				digits:  6,
			},
			want: 383089,
		},
		{
			name: "Test2",
			args: args{
				key:     GenerateTOTPSecret([]byte("abcdefghijklmnopqrstuvwxyz")),
				counter: ts,
				digits:  6,
			},
			want: 269095,
		},
		{
			name: "Test3",
			args: args{
				key:     GenerateTOTPSecret([]byte("!@#$%^&*()_+-=[]{}|;':,.<>/?`~")),
				counter: ts,
				digits:  6,
			},
			want: 203356,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateTOTPCode(tt.args.key, tt.args.counter, tt.args.digits); got != tt.want {
				t.Errorf("GenerateTOTPCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
