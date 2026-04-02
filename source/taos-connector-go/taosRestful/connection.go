package taosRestful

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

type taosConn struct {
	cfg            *Config
	client         *http.Client
	url            *url.URL
	timezone       *time.Location
	timezoneStr    string
	baseRawQuery   string
	header         map[string][]string
	readBufferSize int
}

func newTaosConn(cfg *Config) (*taosConn, error) {
	readBufferSize := cfg.ReadBufferSize
	if readBufferSize <= 0 {
		readBufferSize = 4 << 10
	}
	tc := &taosConn{
		cfg:            cfg,
		timezone:       cfg.Timezone,
		readBufferSize: readBufferSize,
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    cfg.DisableCompression,
	}
	if cfg.SkipVerify {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	tc.client = &http.Client{
		Transport: transport,
	}
	path := "/rest/sql"
	if len(cfg.DbName) != 0 {
		path = fmt.Sprintf("%s/%s", path, cfg.DbName)
	}
	host := net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.Port))
	tc.url = &url.URL{
		Scheme: cfg.Net,
		Host:   host,
		Path:   path,
	}
	tc.header = map[string][]string{
		"Connection": {"keep-alive"},
	}
	baseRawQueryBuilder := strings.Builder{}
	baseRawQueryBuilder.WriteString("app=")
	processName := common.GetProcessName()
	baseRawQueryBuilder.WriteString(url.QueryEscape(processName))
	// cloud token has higher priority
	if cfg.Token != "" {
		baseRawQueryBuilder.WriteString("&token=")
		baseRawQueryBuilder.WriteString(cfg.Token)
	} else if cfg.BearerToken != "" {
		// bearer token has second priority
		tc.header["Authorization"] = []string{fmt.Sprintf("Bearer %s", cfg.BearerToken)}
	} else {
		basic := base64.StdEncoding.EncodeToString([]byte(cfg.User + ":" + cfg.Passwd))
		tc.header["Authorization"] = []string{fmt.Sprintf("Basic %s", basic)}
	}
	if !cfg.DisableCompression {
		tc.header["Accept-Encoding"] = []string{"gzip"}
	}
	if cfg.Timezone != nil {
		tc.timezoneStr = url.QueryEscape(cfg.Timezone.String())
		baseRawQueryBuilder.WriteString("&conn_tz=")
		baseRawQueryBuilder.WriteString(tc.timezoneStr)
	}
	tc.baseRawQuery = baseRawQueryBuilder.String()
	return tc, nil
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	tc.client = nil
	tc.url = nil
	tc.cfg = nil
	tc.header = nil
	return nil
}

func (tc *taosConn) Prepare(query string) (driver.Stmt, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support stmt"}
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	return tc.execCtx(ctx, query, args)
}

func (tc *taosConn) execCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		if !tc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra round trips for preparing and closing a statement
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	result, err := tc.taosQuery(ctx, query, 512)
	if err != nil {
		return nil, err
	}
	if len(result.Data) != 1 || len(result.Data[0]) != 1 {
		return nil, errors.New("wrong result")
	}
	return driver.RowsAffected(result.Data[0][0].(int32)), nil
}

func (tc *taosConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	return tc.queryCtx(ctx, query, args)
}

func (tc *taosConn) queryCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		if !tc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce round trip
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	result, err := tc.taosQuery(ctx, query, tc.readBufferSize)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("wrong result")
	}
	// Read Result
	rs := &rows{
		result: result,
	}
	return rs, err
}

func (tc *taosConn) Ping(ctx context.Context) (err error) {
	return nil
}

func (tc *taosConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support transaction"}
}

func (tc *taosConn) taosQuery(ctx context.Context, sql string, bufferSize int) (*common.TDEngineRestfulResp, error) {
	reqIDValue, err := common.GetReqIDFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if reqIDValue == 0 {
		reqIDValue = common.GetReqID()
	}
	tc.url.RawQuery = fmt.Sprintf("%s&req_id=%d", tc.baseRawQuery, reqIDValue)
	body := ioutil.NopCloser(strings.NewReader(sql))
	req := &http.Request{
		Method:     http.MethodPost,
		URL:        tc.url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     tc.header,
		Body:       body,
		Host:       tc.url.Host,
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("server response: %s - %s", resp.Status, string(body))
	}
	respBody := resp.Body
	defer func() {
		_, _ = ioutil.ReadAll(respBody)
	}()
	if !tc.cfg.DisableCompression && EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		respBody, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	data, err := common.UnmarshalRestfulBodyWithTimezone(respBody, bufferSize, tc.timezone)
	if err != nil {
		return nil, err
	}
	if data.Code != 0 {
		return nil, taosErrors.NewError(data.Code, data.Desc)
	}
	return data, nil
}

// EqualFold is strings.EqualFold, ASCII only. It reports whether s and t
// are equal, ASCII-case-insensitively.
func EqualFold(s, t string) bool {
	if len(s) != len(t) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if lower(s[i]) != lower(t[i]) {
			return false
		}
	}
	return true
}

// lower returns the ASCII lowercase version of b.
func lower(b byte) byte {
	if 'A' <= b && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}
