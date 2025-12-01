package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func TestTransferTaosdClusterBasicInfo(t *testing.T) {
	config.InitConfig()

	conn, _ := db.NewConnector("root", "taosdata", "127.0.0.1", 6041, false)
	defer conn.Close()

	dbName := "db_202412031539"
	conn.Exec(context.Background(), "create database "+dbName, util.GetQidOwn(config.Conf.InstanceID))
	defer conn.Exec(context.Background(), "drop database "+dbName, util.GetQidOwn(config.Conf.InstanceID))

	conn, _ = db.NewConnectorWithDb("root", "taosdata", "127.0.0.1", 6041, dbName, false)
	defer conn.Close()

	cmd := Command{conn: conn, fromTime: time.Now().Add(time.Duration(1 * time.Hour))}
	cmd.TransferTaosdClusterBasicInfo()

	testCases := []struct {
		ep      string
		wantErr bool
	}{
		{"", false},
		{"hello", false},
		{strings.Repeat("a", 128), false},
		{strings.Repeat("a", 255), false},
		{strings.Repeat("a", 256), true},
	}

	conn.Exec(context.Background(),
		"create table d0 using taosd_cluster_basic tags('cluster_id')", util.GetQidOwn(config.Conf.InstanceID))

	for _, tc := range testCases {
		sql := fmt.Sprintf("insert into d0 (ts, first_ep) values(%d, '%s')", time.Now().UnixMilli(), tc.ep)
		_, err := conn.Exec(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
		if tc.wantErr {
			assert.Error(t, err) // [0x2653] Value too long for column/tag: endpoint
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestCommandProcess_BothTransferAndDropSet_ReturnsEarly_NoPanic(t *testing.T) {
	c := &Command{}

	conf := &config.Config{
		Transfer: "old_taosd_metric",
		Drop:     "old_taosd_metric_stables",
	}

	assert.NotPanics(t, func() { c.Process(conf) })
}

func TestCommandProcess_TransferUnsupported_ReturnsEarly_NoPanic(t *testing.T) {
	cmd := &Command{}
	conf := &config.Config{
		Transfer: "not_supported_value",
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("should return early without panic, got panic: %v", r)
		}
	}()

	cmd.Process(conf)
}

func TestCommandProcess_DropUnsupported_ReturnsEarly_NoPanic(t *testing.T) {
	c := &Command{}
	conf := &config.Config{
		Drop: "not_supported_value",
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("should return early without panic, got panic: %v", r)
		}
	}()

	c.Process(conf)
}

func TestCommandProcessTransfer_ParseFromTimeError_ReturnsEarly(t *testing.T) {
	c := &Command{}

	conf := &config.Config{
		FromTime: "not-a-rfc3339",
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("should return early without panic, got panic: %v", r)
		}
	}()

	c.ProcessTransfer(conf)

	if !c.fromTime.IsZero() {
		t.Fatalf("fromTime should remain zero on parse error, got %v", c.fromTime)
	}
}

func TestCommandTransferDataToDest(t *testing.T) {
	cmd := &Command{}
	payload := &db.Data{
		Head: []string{"tag1", "metric1", "ts"},
		Data: [][]interface{}{},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("should return early without panic, got panic: %v", r)
		}
	}()

	cmd.TransferDataToDest(payload, "dst_table", 1)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newCmdWithCapture(bodyOut *string) *Command {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		if bodyOut != nil {
			*bodyOut = string(b)
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(bytes.NewBuffer(nil)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	return &Command{
		client:   &http.Client{Transport: rt},
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}
}

func TestCommandTransferDataToDest_TagInt_WritesEscapedTag(t *testing.T) {
	var posted string
	cmd := newCmdWithCapture(&posted)

	now := time.UnixMilli(1710000000000)
	payload := &db.Data{
		Head: []string{"tag1", "ts"},
		Data: [][]interface{}{
			{int(42), now},
		},
	}

	cmd.TransferDataToDest(payload, "dst", 1)

	if posted == "" {
		t.Fatalf("expected body to be posted, got empty")
	}
	if !strings.Contains(posted, "dst,tag1=42 ") {
		t.Fatalf("missing int tag output, got %q", posted)
	}
}

func TestCommandTransferDataToDest_EmptyStringTag_WritesUnknown(t *testing.T) {
	var posted string
	cmd := newCmdWithCapture(&posted)

	now := time.UnixMilli(1710000000001)
	payload := &db.Data{
		Head: []string{"tag1", "ts"},
		Data: [][]interface{}{
			{"", now},
		},
	}

	cmd.TransferDataToDest(payload, "dst", 1)

	if posted == "" {
		t.Fatalf("expected body to be posted, got empty")
	}
	if !strings.Contains(posted, "dst,tag1=unknown ") {
		t.Fatalf("missing unknown tag output, got %q", posted)
	}
}

func TestCommandTransferDataToDest_MetricInt_WritesFloatF64Suffix(t *testing.T) {
	var posted string
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(req.Body)
			posted = string(b)
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewBuffer(nil)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	cmd := &Command{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}

	ts := time.UnixMilli(1710000000000)
	data := &db.Data{
		Head: []string{"m1", "ts"},
		Data: [][]interface{}{
			{int(42), ts},
		},
	}

	cmd.TransferDataToDest(data, "dst", 0)

	if posted == "" {
		t.Fatalf("expected body to be posted, got empty")
	}
	if !strings.Contains(posted, "m1=42.000000f64") {
		t.Fatalf("missing formatted metric 'm1=42.000000f64', got %q", posted)
	}
	if !strings.HasPrefix(posted, "dst ") || !strings.HasSuffix(posted, "\n") {
		t.Fatalf("unexpected line protocol shape: %q", posted)
	}
}

func TestCommandTransferDataToDest_FlushBranch_Trace_Log_CallWrite_Reset(t *testing.T) {
	oldLevel := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(oldLevel)

	oldMax := MAX_SQL_LEN
	MAX_SQL_LEN = 1
	defer func() { MAX_SQL_LEN = oldMax }()

	posted := ""
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(req.Body)
			posted = string(b)
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewBuffer(nil)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	cmd := &Command{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}

	ts := time.UnixMilli(1710000000000)
	data := &db.Data{
		Head: []string{"tag1", "m1", "ts"},
		Data: [][]interface{}{
			{"x", int(1), ts},
		},
	}

	cmd.TransferDataToDest(data, "dst", 1)

	want := "dst,tag1=x m1=1.000000f64 1710000000000\n"
	if posted != want {
		t.Fatalf("posted body mismatch:\nwant: %q\ngot:  %q", want, posted)
	}
}

func TestCommandTransferDataToDest_FinalFlush_TracefExecuted(t *testing.T) {
	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	var posted string
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(req.Body)
			posted = string(b)
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewBuffer(nil)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	cmd := &Command{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}

	ts := time.UnixMilli(1710000000000)
	data := &db.Data{
		Head: []string{"tag1", "m1", "ts"},
		Data: [][]interface{}{
			{"x", int(1), ts},
		},
	}

	cmd.TransferDataToDest(data, "dst", 1)

	want := "dst,tag1=x m1=1.000000f64 1710000000000\n"
	if posted != want {
		t.Fatalf("posted body mismatch:\nwant: %q\ngot:  %q", want, posted)
	}
}

func TestCommand_lineWriteBody_ClientDoError_ReturnsError(t *testing.T) {
	sentinel := errors.New("net boom")
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, sentinel
	})}

	cmd := &Command{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}

	var buf bytes.Buffer
	buf.WriteString("m  1\n")

	err := cmd.lineWriteBody(&buf)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

func TestCommand_lineWriteBody__UnexpectedStatus_ReadsBodyAndReturnsError(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewBufferString("boom")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	cmd := &Command{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme: "http",
			Host:   "example.com",
			Path:   "/influxdb/v1/write",
		},
	}

	var buf bytes.Buffer
	buf.WriteString("m  1\n")

	err := cmd.lineWriteBody(&buf)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	want := "unexpected status code 400:body:boom"
	if err.Error() != want {
		t.Fatalf("error mismatch:\n  got:  %q\n  want: %q", err.Error(), want)
	}
}
