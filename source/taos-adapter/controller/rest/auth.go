package rest

import (
	"crypto/des"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

var authCache = cache.New(30*time.Minute, time.Hour)
var tokenCache = cache.New(30*time.Minute, time.Hour)

type authInfo struct {
	User     string
	Password string
}

var desKey = []byte{
	64,
	182,
	122,
	48,
	86,
	115,
	253,
	68,
}

func DecodeDes(auth string) (user, password string, err error) {
	d, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", "", err
	}
	if len(d) < 48 || len(d)%8 != 0 {
		return "", "", errors.New("wrong des length")
	}
	batch := len(d) / 8
	block, _ := des.NewCipher(desKey)
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	for i := 0; i < batch; i++ {
		origData := make([]byte, 8)
		block.Decrypt(origData, d[i*8:+(i+1)*8])
		b.Write(origData)
		// user is 24 bytes
		if i == 2 {
			user, err = b.ReadString(0)
			if err == nil {
				user = user[:len(user)-1]
			}
			b.Reset()
		}
	}
	password, err = b.ReadString(0)
	if err == nil {
		password = password[:len(password)-1]
	}
	return user, password, nil
}

func EncodeDes(user, password string) (string, error) {
	if len(user) > 24 {
		return "", errors.New("wrong user length")
	}
	var key string
	buf := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buf)
	totalLen := 48
	if len(password) > 24 {
		totalLen = nearestMultipleOfEight(24 + len(password))
	}
	b := make([]byte, totalLen)
	// user
	copy(b, user)
	// password
	copy(b[24:], password)
	key = string(b)
	v, exist := tokenCache.Get(key)
	if exist {
		return v.(string), nil
	}

	block, _ := des.NewCipher(desKey)
	batch := totalLen / 8
	for i := 0; i < batch; i++ {
		d := make([]byte, 8)
		block.Encrypt(d, b[i*8:(i+1)*8])
		buf.Write(d)
	}
	data := base64.StdEncoding.EncodeToString(buf.Bytes())
	if len(key) != 0 {
		tokenCache.SetDefault(key, data)
	}
	return data, nil
}

func nearestMultipleOfEight(n int) int {
	return (n + 7) / 8 * 8
}

const (
	UserKey     = "user"
	PasswordKey = "password"
	TokenKey    = "bearer_token"
)

func CheckAuth(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	auth := c.GetHeader("Authorization")
	if len(auth) == 0 {
		UnAuthResponse(c, logger, httperror.HTTP_NO_AUTH_INFO)
		return
	}
	_, cloudTokenExists := c.GetQuery("token")
	auth = strings.TrimSpace(auth)
	v, exist := authCache.Get(auth)
	if exist {
		info := v.(*authInfo)
		c.Set(UserKey, info.User)
		c.Set(PasswordKey, info.Password)
		return
	}
	if strings.HasPrefix(auth, "Basic ") && len(auth) > 6 {
		user, password, err := tools.DecodeBasic(auth[6:])
		if err != nil {
			UnAuthResponse(c, logger, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		if len(user) == 0 || len(password) == 0 {
			UnAuthResponse(c, logger, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		authCache.SetDefault(auth, &authInfo{
			User:     user,
			Password: password,
		})
		c.Set(UserKey, user)
		c.Set(PasswordKey, password)
	} else if strings.HasPrefix(auth, "Taosd ") && len(auth) > 6 {
		user, password, err := DecodeDes(auth[6:])
		if err != nil {
			UnAuthResponse(c, logger, httperror.HTTP_INVALID_TAOSD_AUTH)
			return
		}
		if len(user) == 0 || len(password) == 0 {
			UnAuthResponse(c, logger, httperror.HTTP_INVALID_TAOSD_AUTH)
			return
		}
		authCache.SetDefault(auth, &authInfo{
			User:     user,
			Password: password,
		})
		c.Set(UserKey, user)
		c.Set(PasswordKey, password)
	} else if !cloudTokenExists && strings.HasPrefix(auth, "Bearer ") && len(auth) > 7 {
		token := strings.TrimSpace(auth[7:])
		c.Set(UserKey, "")
		c.Set(PasswordKey, "")
		c.Set(TokenKey, token)
	} else {
		UnAuthResponse(c, logger, httperror.HTTP_INVALID_AUTH_TYPE)
		return
	}
}

func getAuthInfo(c *gin.Context) (user, password, token string) {
	u, exist := c.Get(UserKey)
	if exist {
		user = u.(string)
	}
	p, exist := c.Get(PasswordKey)
	if exist {
		password = p.(string)
	}
	t, exist := c.Get(TokenKey)
	if exist {
		token = t.(string)
	}
	return user, password, token
}

type Message struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}
