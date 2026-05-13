package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:09
// @description: test encode des
func TestEncodeDes(t *testing.T) {
	type args struct {
		user     string
		password string
	}
	password255 := make([]byte, 255)
	for i := 0; i < 254; i++ {
		password255[i] = 'a'
	}
	password255[254] = 'b'
	password255Str := string(password255)
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				user:     "root",
				password: "taosdata",
			},
			want:    "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04",
			wantErr: false,
		},
		{
			name: "wrong length",
			args: args{
				user:     "aaaaaaaaaaaaaaaaaaaaaaaaa",
				password: "",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "24 password",
			args: args{
				user:     "test",
				password: "aaaaaaaaaaaaaaaaaaaaaaaa",
			},
			want:    "QIIxd+q+/t7a8qdtNZmtONryp201ma040a9W6u3942bRr1bq7f3jZtGvVurt/eNm",
			wantErr: false,
		},
		{
			name: "255 password",
			args: args{
				user:     "test",
				password: password255Str,
			},
			want:    "QIIxd+q+/t7a8qdtNZmtONryp201ma040a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942Yo7IixueqHsw==",
			wantErr: false,
		},
		{
			name: "special character",
			args: args{
				user:     "!@#$%^&*()-_+=[]{}:;><?|",
				password: "!@#$%^&*()-_+=[]{}:;><?|~,.",
			},
			want:    "/V5LPu+ZLfbmj8ShH4U/6GWQdrL94Zwy/V5LPu+ZLfbmj8ShH4U/6GWQdrL94Zwyg97Qep/1RAM=",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				got, err := EncodeDes(tt.args.user, tt.args.password)
				if (err != nil) != tt.wantErr {
					t.Errorf("EncodeDes() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if got != tt.want {
					t.Errorf("EncodeDes() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:10
// @description: test decode des
func TestDecodeDes(t *testing.T) {
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
			name: "test",
			args: args{
				auth: "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04",
			},
			wantUser:     "root",
			wantPassword: "taosdata",
			wantErr:      false,
		}, {
			name: "wrong",
			args: args{
				auth: "/",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		}, {
			name: "wrong length",
			args: args{
				auth: "YQ==",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		},
		{
			name: "24 password",
			args: args{
				auth: "QIIxd+q+/t7a8qdtNZmtONryp201ma040a9W6u3942bRr1bq7f3jZtGvVurt/eNm",
			},
			wantUser:     "test",
			wantPassword: "aaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr:      false,
		},
		{
			name: "255 password",
			args: args{
				auth: "QIIxd+q+/t7a8qdtNZmtONryp201ma040a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942bRr1bq7f3jZtGvVurt/eNm0a9W6u3942Yo7IixueqHsw==",
			},
			wantUser:     "test",
			wantPassword: password255Str,
			wantErr:      false,
		},
		{
			name: "special character",
			args: args{
				auth: "/V5LPu+ZLfbmj8ShH4U/6GWQdrL94Zwy/V5LPu+ZLfbmj8ShH4U/6GWQdrL94Zwyg97Qep/1RAM=",
			},
			wantUser:     "!@#$%^&*()-_+=[]{}:;><?|",
			wantPassword: "!@#$%^&*()-_+=[]{}:;><?|~,.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPassword, err := DecodeDes(tt.args.auth)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeDes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotUser != tt.wantUser {
				fmt.Println(len(gotUser))
				t.Errorf("DecodeDes() gotUser = %v, want %v", gotUser, tt.wantUser)
			}
			if gotPassword != tt.wantPassword {
				t.Errorf("DecodeDes() gotPassword = %v, want %v", gotPassword, tt.wantPassword)
			}
		})
	}
}

func BenchmarkEncodeDes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s, err := EncodeDes("root", "taosdata")
		if err != nil {
			b.Error(err)
			return
		}
		_ = s
	}
}

func BenchmarkDecodeDes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := DecodeDes("/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func Test_nearestMultipleOfEight(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "0",
			args: args{
				n: 0,
			},
			want: 0,
		},
		{
			name: "1",
			args: args{
				n: 1,
			},
			want: 8,
		},
		{
			name: "8",
			args: args{
				n: 8,
			},
			want: 8,
		},
		{
			name: "9",
			args: args{
				n: 9,
			},
			want: 16,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, nearestMultipleOfEight(tt.args.n), "nearestMultipleOfEight(%v)", tt.args.n)
		})
	}
}

func TestAuth(t *testing.T) {
	logger := log.GetLogger("test")
	router := gin.Default()
	setLogger := func(c *gin.Context) {
		c.Set(LoggerKey, logger)
	}
	router.GET("/", setLogger, CheckAuth, func(c *gin.Context) {
		u, p, token := getAuthInfo(c)
		assert.Equal(t, u, "root")
		assert.Equal(t, p, "taosdata")
		assert.Equal(t, token, "")
		c.Status(200)
	})
	router.GET("/token_test", setLogger, CheckAuth, func(c *gin.Context) {
		u, p, token := getAuthInfo(c)
		assert.Equal(t, u, "")
		assert.Equal(t, p, "")
		assert.Equal(t, token, "test_token")
		c.Status(200)
	})
	router.GET("/unknown", setLogger, CheckAuth, func(c *gin.Context) {
		u, p, token := getAuthInfo(c)
		assert.Equal(t, "", u)
		assert.Equal(t, "", p)
		assert.Equal(t, "", token)
		c.Status(401)
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic wrong_basic")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/token_test", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Bearer test_token")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/unknown", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Bearerx test_token")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/unknown", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("", "")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)
}
