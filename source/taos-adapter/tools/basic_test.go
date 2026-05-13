package tools

import "testing"

func BenchmarkDecodeBasic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := DecodeBasic("cm9vdDp0YW9zZGF0YQ==")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// @author: xftan
// @date: 2021/12/14 15:17
// @description: test decode Basic
func TestDecodeBasic(t *testing.T) {
	type args struct {
		auth string
	}
	password255 := make([]byte, 255)
	for i := 0; i < 254; i++ {
		password255[i] = 'a'
	}
	password255[254] = 'b'
	password255Str := string(password255)
	tests := []struct {
		name         string
		args         args
		wantUser     string
		wantPassword string
		wantErr      bool
	}{
		{
			name: "root",
			args: args{
				auth: "cm9vdDp0YW9zZGF0YQ==",
			},
			wantUser:     "root",
			wantPassword: "taosdata",
			wantErr:      false,
		},
		{
			name: "wrong base64",
			args: args{
				auth: "wrong base64",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		},
		{
			name: "wrong split",
			args: args{
				auth: "cm9vdHRhb3NkYXRh",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		},
		{
			name: "special char",
			args: args{
				auth: "dGVzdDoxIXFAIyQlXiYqKCktXys9W117fTo7Pjw/fH4sLg==",
			},
			wantUser:     "test",
			wantPassword: "1!q@#$%^&*()-_+=[]{}:;><?|~,.",
			wantErr:      false,
		},
		{
			name: "password 255",
			args: args{
				auth: "dGVzdDphYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWI=",
			},
			wantUser:     "test",
			wantPassword: password255Str,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPassword, err := DecodeBasic(tt.args.auth)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeBasic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotUser != tt.wantUser {
				t.Errorf("DecodeBasic() gotUser = %v, want %v", gotUser, tt.wantUser)
			}
			if gotPassword != tt.wantPassword {
				t.Errorf("DecodeBasic() gotPassword = %v, want %v", gotPassword, tt.wantPassword)
			}
		})
	}
}
