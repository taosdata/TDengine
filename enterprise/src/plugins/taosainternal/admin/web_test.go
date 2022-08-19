package admin

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/db"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	config.Init()
	db.PrepareConnection()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	router.Use(func(context *gin.Context) {
		context.Set("currentID", uint32(0))
	})
	var ctl Controller
	ctl.Init(router)
	m.Run()
}

// @author: xftan
// @date: 2021/12/24 13:30
// @description: test admin/info get
func TestInfoGet(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/admin/info", nil)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/24 13:31
// @description: test admin/info post
func TestInfoPost(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/admin/info", nil)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/24 13:31
// @description: test admin/meta
func TestMeta(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("select * from log.dnodes_info")
	req, _ := http.NewRequest(http.MethodPost, "/admin/meta", body)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/24 13:31
// @description: test /admin/login
func TestLogin(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/admin/login", nil)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var data rest.Message
	err := json.Unmarshal(w.Body.Bytes(), &data)
	assert.NoError(t, err)
	assert.Equal(t, "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04", data.Desc)
}

// @author: xftan
// @date: 2021/12/24 13:31
// @description: test admin/login
func TestLogout(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/admin/login", nil)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/24 13:32
// @description: test admin/sql
func TestSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("show databases")
	req, _ := http.NewRequest(http.MethodPost, "/admin/sql", body)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestDownload(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("show databases")
	req, _ := http.NewRequest(http.MethodPost, "/admin/result", body)
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	r := csv.NewReader(w.Body)
	data, err := r.ReadAll()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(data), 1)
	assert.Equal(
		t,
		[]string{
			"name",
		},
		data[0],
	)
}
