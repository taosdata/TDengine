package plugin

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

const (
	UserKey     = "user"
	PasswordKey = "password"
	TokenKey    = "bearer_token"
)

type authInfo struct {
	User     string
	Password string
}

var authCache = cache.New(30*time.Minute, time.Hour)

func Auth(errHandler func(c *gin.Context, code int, err error)) func(c *gin.Context) {
	return func(c *gin.Context) {
		auth := c.GetHeader("Authorization")
		if len(auth) == 0 {
			errHandler(c, http.StatusUnauthorized, errors.New("auth needed"))
			c.Abort()
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
				errHandler(c, http.StatusUnauthorized, err)
				c.Abort()
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
			c.Set(TokenKey, token)
		} else {
			errHandler(c, http.StatusUnauthorized, fmt.Errorf("unsupported auth type"))
			c.Abort()
			return
		}
	}
}

func RegisterGenerateAuth(r gin.IRouter) {
	r.GET("genauth/:user/:password/:key", func(c *gin.Context) {
		user := c.Param("user")
		password := c.Param("password")
		key := c.Param("key")
		if len(user) == 0 || len(user) > 24 || len(password) == 0 || len(key) == 0 {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		b := pool.BytesPoolGet()
		defer pool.BytesPoolPut(b)
		b.WriteString(user)
		b.WriteByte(':')
		b.WriteString(password)
		c.String(http.StatusOK, b.String())
	})
}

func GetAuth(c *gin.Context) (user, password, token string, err error) {
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
	if token == "" && (len(user) == 0 || len(password) == 0) {
		err = errors.New("no auth info found")
	}
	return user, password, token, err
}
