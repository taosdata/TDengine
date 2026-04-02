package rest

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
	"github.com/taosdata/taosadapter/v3/version"
)

func TestChangeConfig(t *testing.T) {
	baseLevel := log.GetLogLevel().String()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPut, "/config", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// no body
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong log.level
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"log.level":"wrong"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// success
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()

	body = strings.NewReader(`{"log.level":"warn"}`)
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, logrus.WarnLevel, log.GetLogLevel())
	t.Log(log.GetLogLevel())

	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`{"log.level":"%s"}`, baseLevel))
	req, _ = http.NewRequest(http.MethodPut, "/config", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
}

func TestRecordSql(t *testing.T) {
	//log.SetLevel("debug")
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong config
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"start_time":"xxx"}`)
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	assert.Equal(t, `{"code":0,"desc":"","exists":false,"running":false,"start_time":"","end_time":"","current_concurrent":0}`, w.Body.String())

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, `{"code":0,"desc":""}`, w.Body.String())

	// start record sql with start time and end time
	tmpDir := t.TempDir()
	oldPath := config.Conf.Log.Path
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	start := time.Now().Format(recordsql.InputTimeFormat)
	end := time.Now().Add(time.Minute).Format(recordsql.InputTimeFormat)
	body = strings.NewReader(fmt.Sprintf(`{"start_time":"%s","end_time":"%s"}`, start, end))
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath

	time.Sleep(time.Millisecond * 10)

	// execute sql
	sqlRequestAddr := testtools.GetRandomRemoteAddr()
	host, port, err := net.SplitHostPort(strings.TrimSpace(sqlRequestAddr))
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testapp", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	var stateResp GetRecordSqlStateResp
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	assert.Equal(t, start, stateResp.StartTime)
	assert.Equal(t, end, stateResp.EndTime)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	var stopResp StopRecordResp
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stopResp.StartTime, start)
	assert.Equal(t, stopResp.EndTime, end)

	// check record sql file
	files, err := getRecordFiles(tmpDir, "Sql")
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	t.Log(files)
	recordFile := filepath.Join(tmpDir, files[0])
	recordContent, err := os.ReadFile(recordFile)
	assert.NoError(t, err)
	csvReader := csv.NewReader(bytes.NewReader(recordContent))
	records, err := csvReader.ReadAll()
	assert.NoError(t, err)
	require.Equal(t, 1, len(records), records)
	assert.Equal(t, "show databases", records[0][recordsql.SQLIndex])
	assert.Equal(t, host, records[0][recordsql.IPIndex])
	assert.Equal(t, "root", records[0][recordsql.UserIndex])
	assert.Equal(t, "http", records[0][recordsql.ConnTypeIndex])
	assert.Equal(t, "testapp", records[0][recordsql.AppNameIndex])
	assert.Equal(t, port, records[0][recordsql.SourcePortIndex])

	// start record sql without start time and end time
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath
	t.Log(w.Body.String())

	time.Sleep(time.Millisecond * 10)
	// execute sql
	sqlRequestAddr = testtools.GetRandomRemoteAddr()
	host, port, err = net.SplitHostPort(strings.TrimSpace(sqlRequestAddr))
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testapp", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("create database if not exists test_record_sql")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testapp", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	assert.NoError(t, testtools.EnsureDBCreated("test_record_sql"))

	defer func() {
		w = httptest.NewRecorder()
		body = strings.NewReader("drop database if exists test_record_sql")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testapp", body)
		req.RemoteAddr = sqlRequestAddr
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}()

	// wrong sql
	w = httptest.NewRecorder()
	body = strings.NewReader("xxxx")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testapp", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// without app
	// wrong sql
	w = httptest.NewRecorder()
	body = strings.NewReader("no app name")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = sqlRequestAddr
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	assert.Equal(t, recordsql.DefaultRecordSqlEndTime, stateResp.EndTime)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stateResp.StartTime, stopResp.StartTime)
	assert.Equal(t, recordsql.DefaultRecordSqlEndTime, stopResp.EndTime)

	files, err = getRecordFiles(tmpDir, "Sql")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	t.Log(files)
	recordFile = ""
	for _, file := range files {
		if !strings.HasSuffix(file, ".csv") {
			recordFile = file
		}
	}
	require.True(t, recordFile != "")

	recordFile = filepath.Join(tmpDir, recordFile)
	recordContent, err = os.ReadFile(recordFile)
	assert.NoError(t, err)
	csvReader = csv.NewReader(bytes.NewReader(recordContent))
	records, err = csvReader.ReadAll()
	assert.NoError(t, err)
	require.Equal(t, 4, len(records), records)

	assert.Equal(t, "show databases", records[0][recordsql.SQLIndex])
	assert.Equal(t, host, records[0][recordsql.IPIndex])
	assert.Equal(t, "root", records[0][recordsql.UserIndex])
	assert.Equal(t, "http", records[0][recordsql.ConnTypeIndex])
	assert.Equal(t, "testapp", records[0][recordsql.AppNameIndex])
	assert.Equal(t, port, records[0][recordsql.SourcePortIndex])
	assert.NotEqual(t, "0", records[0][recordsql.GetConnDurationIndex])
	queryDuration, err := strconv.Atoi(records[0][recordsql.QueryDurationIndex])
	assert.NoError(t, err)
	assert.Greater(t, queryDuration, 0)

	assert.Equal(t, "create database if not exists test_record_sql", records[1][recordsql.SQLIndex])
	assert.Equal(t, host, records[1][recordsql.IPIndex])
	assert.Equal(t, "root", records[1][recordsql.UserIndex])
	assert.Equal(t, "http", records[1][recordsql.ConnTypeIndex])
	assert.Equal(t, "testapp", records[1][recordsql.AppNameIndex])
	assert.Equal(t, port, records[1][recordsql.SourcePortIndex])

	assert.Equal(t, "xxxx", records[2][recordsql.SQLIndex])
	assert.Equal(t, host, records[2][recordsql.IPIndex])
	assert.Equal(t, "root", records[2][recordsql.UserIndex])
	assert.Equal(t, "http", records[2][recordsql.ConnTypeIndex])
	assert.Equal(t, "testapp", records[2][recordsql.AppNameIndex])
	assert.Equal(t, port, records[2][recordsql.SourcePortIndex])

	assert.Equal(t, "no app name", records[3][recordsql.SQLIndex])
	assert.Equal(t, host, records[3][recordsql.IPIndex])
	assert.Equal(t, "root", records[3][recordsql.UserIndex])
	assert.Equal(t, "http", records[3][recordsql.ConnTypeIndex])
	assert.Equal(t, "", records[3][recordsql.AppNameIndex])
	assert.Equal(t, port, records[3][recordsql.SourcePortIndex])

	if testenv.IsEnterpriseTest() {
		// create token
		w = httptest.NewRecorder()
		body = strings.NewReader("create token test_token_record_sql from user root")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testtoken", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.SetBasicAuth(user, password)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code, w.Body.String())
		var objResult TDEngineRestfulRespDoc
		err = json.Unmarshal(w.Body.Bytes(), &objResult)
		assert.NoError(t, err)
		assert.Greater(t, len(objResult.Data), 0)
		token := objResult.Data[0][0].(string)
		assert.NotEmpty(t, token)
		//t.Log("got token:", token)
		defer func() {
			w = httptest.NewRecorder()
			body = strings.NewReader("drop token test_token_record_sql")
			req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testtoken", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			req.SetBasicAuth(user, password)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code, w.Body.String())
		}()
		err = testtools.EnsureTokenCreated("test_token_record_sql")
		assert.NoError(t, err)
		// start record
		w = httptest.NewRecorder()
		body = strings.NewReader("")
		req, _ = http.NewRequest(http.MethodPost, "/record_sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.SetBasicAuth(user, password)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code, w.Body.String())
		config.Conf.Log.Path = oldPath
		t.Log(w.Body.String())
		// execute sql
		sqlRequestAddr = testtools.GetRandomRemoteAddr()
		host, port, err = net.SplitHostPort(strings.TrimSpace(sqlRequestAddr))
		assert.NoError(t, err)
		w = httptest.NewRecorder()
		body = strings.NewReader("show databases")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql?app=testtoken", body)
		req.RemoteAddr = sqlRequestAddr
		req.Header.Set("Authorization", "Bearer "+token)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		// get record sql state
		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodGet, "/record_sql", nil)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Bearer "+token)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		t.Log(w.Body.String())
		err = json.Unmarshal(w.Body.Bytes(), &stateResp)
		assert.NoError(t, err)

		w = httptest.NewRecorder()
		req, _ = http.NewRequest(http.MethodDelete, "/record_sql", nil)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Bearer "+token)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code, w.Body.String())
		t.Log(w.Body.String())
		err = json.Unmarshal(w.Body.Bytes(), &stopResp)
		assert.NoError(t, err)
		assert.Equal(t, stateResp.StartTime, stopResp.StartTime)
		assert.Equal(t, recordsql.DefaultRecordSqlEndTime, stopResp.EndTime)

		files, err = getRecordFiles(tmpDir, "Sql")
		require.NoError(t, err)
		require.Equal(t, 3, len(files))
		t.Log(files)
		sort.Strings(files)
		recordFile = files[2]

		recordFile = filepath.Join(tmpDir, recordFile)
		recordContent, err = os.ReadFile(recordFile)
		assert.NoError(t, err)
		csvReader = csv.NewReader(bytes.NewReader(recordContent))
		records, err = csvReader.ReadAll()
		assert.NoError(t, err)
		require.Equal(t, 1, len(records), records)

		record := records[0]
		assert.Equal(t, "show databases", record[recordsql.SQLIndex])
		assert.Equal(t, host, record[recordsql.IPIndex])
		assert.Equal(t, "root", record[recordsql.UserIndex])
		assert.Equal(t, "http", record[recordsql.ConnTypeIndex])
		assert.Equal(t, "testtoken", record[recordsql.AppNameIndex])
		assert.Equal(t, port, record[recordsql.SourcePortIndex])
		assert.NotEqual(t, "0", record[recordsql.GetConnDurationIndex])
		queryDuration, err := strconv.Atoi(record[recordsql.QueryDurationIndex])
		assert.NoError(t, err)
		assert.Greater(t, queryDuration, 0)
	}
}

func getRecordFiles(dir string, prefix string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && strings.HasPrefix(info.Name(), fmt.Sprintf("%sadapter%s_", version.CUS_PROMPT, prefix)) && !strings.HasSuffix(info.Name(), "_lock") {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}

func TestRecordStmt(t *testing.T) {
	//log.SetLevel("debug")
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// wrong password
	user := "root"
	password := "wrong"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code, w.Body.String())

	// whitelist error
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", nil)
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 403, w.Code, w.Body.String())

	// wrong json
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body := strings.NewReader("xxx")
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// wrong config
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader(`{"start_time":"xxx"}`)
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body.String())

	// get record state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	assert.Equal(t, `{"code":0,"desc":"","exists":false,"running":false,"start_time":"","end_time":"","current_concurrent":0}`, w.Body.String())

	// stop record
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	assert.Equal(t, `{"code":0,"desc":""}`, w.Body.String())

	// start record with start time and end time
	tmpDir := t.TempDir()
	oldPath := config.Conf.Log.Path
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	start := time.Now().Format(recordsql.InputTimeFormat)
	end := time.Now().Add(time.Minute).Format(recordsql.InputTimeFormat)
	body = strings.NewReader(fmt.Sprintf(`{"start_time":"%s","end_time":"%s"}`, start, end))
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath

	time.Sleep(time.Millisecond * 10)

	record, recordStmt := recordsql.GetStmtRecord()
	require.True(t, recordStmt)
	randomAddr := testtools.GetRandomRemoteAddr()
	host, port, err := net.SplitHostPort(strings.TrimSpace(randomAddr))
	assert.NoError(t, err)
	app := "testapp"
	qid := uint64(0x1234)
	now := time.Now()
	testsql := "select * from test where name = ?"
	record.InitPrepare(0x1234, host, port, app, user, recordsql.WSType, qid, now, testsql)

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	var stateResp GetRecordSqlStateResp
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	assert.Equal(t, start, stateResp.StartTime)
	assert.Equal(t, end, stateResp.EndTime)
	assert.Equal(t, int32(1), stateResp.CurrentConcurrent)
	record.SetPrepareEnd(0)
	recordsql.PutStmtRecord(record)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	var stopResp StopRecordResp
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stopResp.StartTime, start)
	assert.Equal(t, stopResp.EndTime, end)

	// check record sql file
	files, err := getRecordFiles(tmpDir, "Stmt")
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	t.Log(files)
	recordFile := filepath.Join(tmpDir, files[0])
	recordContent, err := os.ReadFile(recordFile)
	assert.NoError(t, err)
	csvReader := csv.NewReader(bytes.NewReader(recordContent))
	records, err := csvReader.ReadAll()
	assert.NoError(t, err)
	require.Equal(t, 1, len(records), records)
	assert.Equal(t, testsql, records[0][recordsql.StmtDataIndex])
	assert.Equal(t, host, records[0][recordsql.StmtIPIndex])
	assert.Equal(t, "root", records[0][recordsql.StmtUserIndex])
	assert.Equal(t, "ws", records[0][recordsql.StmtConnTypeIndex])
	assert.Equal(t, "testapp", records[0][recordsql.StmtAppNameIndex])
	assert.Equal(t, port, records[0][recordsql.StmtSourcePortIndex])
	assert.Equal(t, "prepare", records[0][recordsql.StmtActionIndex])

	// start record sql without start time and end time
	config.Conf.Log.Path = tmpDir
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	body = strings.NewReader("")
	req, _ = http.NewRequest(http.MethodPost, "/record_stmt", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	config.Conf.Log.Path = oldPath
	t.Log(w.Body.String())

	time.Sleep(time.Millisecond * 10)
	// execute sql

	record, recordStmt = recordsql.GetStmtRecord()
	require.True(t, recordStmt)
	record.InitBind(0x1234, host, port, app, user, recordsql.WSType, qid, now, []byte{0x01, 0x02, 0x03})

	// get record sql state
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stateResp)
	assert.NoError(t, err)
	startTime, err := time.Parse(recordsql.InputTimeFormat, stateResp.StartTime)
	assert.NoError(t, err)
	endTime := startTime.Add(time.Hour).Format(recordsql.InputTimeFormat)
	assert.Equal(t, endTime, stateResp.EndTime)
	assert.Equal(t, int32(1), stateResp.CurrentConcurrent)
	record.SetBindEnd(0)

	// stop record sql
	user = "root"
	password = "taosdata"
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, "/record_stmt", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth(user, password)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	t.Log(w.Body.String())
	err = json.Unmarshal(w.Body.Bytes(), &stopResp)
	assert.NoError(t, err)
	assert.Equal(t, stateResp.StartTime, stopResp.StartTime)
	assert.Equal(t, endTime, stopResp.EndTime)

	files, err = getRecordFiles(tmpDir, "Stmt")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	t.Log(files)
	recordFile = ""
	for _, file := range files {
		if !strings.HasSuffix(file, ".csv") {
			recordFile = file
		}
	}
	require.True(t, recordFile != "")

	recordFile = filepath.Join(tmpDir, recordFile)
	recordContent, err = os.ReadFile(recordFile)
	assert.NoError(t, err)
	csvReader = csv.NewReader(bytes.NewReader(recordContent))
	records, err = csvReader.ReadAll()
	assert.NoError(t, err)
	t.Log(records)
	require.Equal(t, 1, len(records), records)

	assert.Equal(t, "010203", records[0][recordsql.StmtDataIndex])
	assert.Equal(t, host, records[0][recordsql.StmtIPIndex])
	assert.Equal(t, "root", records[0][recordsql.StmtUserIndex])
	assert.Equal(t, "ws", records[0][recordsql.StmtConnTypeIndex])
	assert.Equal(t, app, records[0][recordsql.StmtAppNameIndex])
	assert.Equal(t, port, records[0][recordsql.StmtSourcePortIndex])
	assert.Equal(t, "bind", records[0][recordsql.StmtActionIndex])
	duration, err := strconv.Atoi(records[0][recordsql.StmtDurationIndex])
	assert.NoError(t, err)
	assert.Greater(t, duration, 0)
}
