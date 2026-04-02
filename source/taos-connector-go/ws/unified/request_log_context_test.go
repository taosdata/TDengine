package unified

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestBuildTextRequestSummaryRedactsSensitiveFields verifies the expected behavior for this scenario.
func TestBuildTextRequestSummaryRedactsSensitiveFields(t *testing.T) {
	args := []byte(`{
		"user":"root",
		"password":"plain-password",
		"token":"plain-token",
		"td.connect.token":"cfg-token",
		"url":"ws://127.0.0.1:6041/rest/tmq?token=query-token&x=1",
		"sql":"select * from t where password='secret-pass' and token=\"secret-token\"",
		"safe":"ok"
	}`)
	summary := buildTextRequestSummary("subscribe", 101, args)

	require.Contains(t, summary, "action=subscribe")
	require.Contains(t, summary, "req_id=101")
	require.Contains(t, summary, `"password":"***"`)
	require.Contains(t, summary, `"token":"***"`)
	require.Contains(t, summary, `"td.connect.token":"***"`)
	require.Contains(t, summary, `"safe":"ok"`)
	require.NotContains(t, summary, "plain-password")
	require.NotContains(t, summary, "plain-token")
	require.NotContains(t, summary, "cfg-token")
	require.NotContains(t, summary, "query-token")
	require.NotContains(t, summary, "secret-pass")
	require.NotContains(t, summary, "secret-token")
}

// TestBuildTextRequestSummaryDoesNotRedactNonSensitiveOtpSubstrings verifies keys containing "otp" as plain substring are not over-redacted.
func TestBuildTextRequestSummaryDoesNotRedactNonSensitiveOtpSubstrings(t *testing.T) {
	args := []byte(`{
		"bootstrap":"yes",
		"footprint":"trace",
		"otp":"123456",
		"otp_code":"654321"
	}`)
	summary := buildTextRequestSummary("conn", 202, args)

	require.Contains(t, summary, `"bootstrap":"yes"`)
	require.Contains(t, summary, `"footprint":"trace"`)
	require.Contains(t, summary, `"otp":"***"`)
	require.Contains(t, summary, `"otp_code":"***"`)
	require.NotContains(t, summary, `"otp":"123456"`)
	require.NotContains(t, summary, `"otp_code":"654321"`)
}

// TestBuildBinaryQueryRequestSummaryRedactsSensitiveText verifies the expected behavior for this scenario.
func TestBuildBinaryQueryRequestSummaryRedactsSensitiveText(t *testing.T) {
	sql := `insert into t values(now, 1) password='abc' token=def authorization:"ghi"`
	summary := buildBinaryQueryRequestSummary(88, sql)

	require.Contains(t, summary, "binary_action=query")
	require.Contains(t, summary, "req_id=88")
	require.Contains(t, summary, "password=***")
	require.Contains(t, summary, "token=***")
	require.Contains(t, summary, "authorization:***")
	require.NotContains(t, summary, "abc")
	require.NotContains(t, summary, "def")
	require.NotContains(t, summary, "ghi")
}

func TestRedactURLStringForLogRedactsUserInfoPassword(t *testing.T) {
	sanitized, ok := redactURLStringForLog("ws://alice:raw-pass@127.0.0.1:6041/ws?x=1")
	require.True(t, ok)
	parsed, err := url.Parse(sanitized)
	require.NoError(t, err)
	require.NotNil(t, parsed.User)
	password, has := parsed.User.Password()
	require.True(t, has)
	require.Equal(t, "***", password)
	require.Equal(t, "alice", parsed.User.Username())
	require.NotContains(t, sanitized, "raw-pass")
}

// TestWrapRequestErrorKeepsErrorIs verifies wrapped errors still support errors.Is checks.
func TestWrapRequestErrorKeepsErrorIs(t *testing.T) {
	wrapped := wrapRequestError(client.ClosedError, "action=ping")
	require.Error(t, wrapped)
	require.True(t, errors.Is(wrapped, client.ClosedError))
	require.Contains(t, wrapped.Error(), "request=action=ping")
}

// TestWrapRequestErrorSummaryFuncLazy verifies summary function is evaluated only when needed.
func TestWrapRequestErrorSummaryFuncLazy(t *testing.T) {
	called := 0
	summaryFunc := func() string {
		called++
		return "action=query"
	}

	require.NoError(t, wrapRequestErrorWithSummaryFunc(nil, summaryFunc))
	require.Equal(t, 0, called)

	wrapped := wrapRequestErrorWithSummaryFunc(client.ClosedError, summaryFunc)
	require.Error(t, wrapped)
	require.Equal(t, 1, called)
	require.Contains(t, wrapped.Error(), "request=action=query")
}

// TestWrapRequestErrorPreservesNonUnifiedCause verifies non-unified causes remain discoverable via errors.Is.
func TestWrapRequestErrorPreservesNonUnifiedCause(t *testing.T) {
	wrapped := wrapRequestError(io.ErrClosedPipe, "action=query")
	require.Error(t, wrapped)
	require.True(t, errors.Is(wrapped, io.ErrClosedPipe))
}

// TestWrapRequestErrorPreservesUnifiedMetadata verifies helper metadata survives wrapping with request summary.
func TestWrapRequestErrorPreservesUnifiedMetadata(t *testing.T) {
	cause := &Error{
		Type:              ErrorTypeMessageTimeout,
		Message:           "query message timeout",
		ConnectionRelated: true,
	}
	wrappedCause := fmt.Errorf("send failed: %w", cause)
	wrapped := wrapRequestErrorWithSummaryFunc(wrappedCause, fixedSummaryFunc("action=query req_id=10"))
	require.Error(t, wrapped)
	require.True(t, IsErrorType(wrapped, ErrorTypeMessageTimeout))
	require.True(t, IsConnectionRelatedError(wrapped))
	require.True(t, errors.Is(wrapped, cause))
	require.Contains(t, wrapped.Error(), "query message timeout")
	require.Contains(t, wrapped.Error(), "request=action=query req_id=10")
}

// TestWrapRequestErrorDoesNotDuplicateRequestSummary verifies repeated wrapping does not append duplicate summary text.
func TestWrapRequestErrorDoesNotDuplicateRequestSummary(t *testing.T) {
	first := wrapRequestError(io.ErrClosedPipe, "action=query")
	second := wrapRequestError(first, "action=query")
	require.Equal(t, 1, strings.Count(second.Error(), "request=action=query"))
}
