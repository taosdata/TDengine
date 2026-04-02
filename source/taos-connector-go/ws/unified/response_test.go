package unified

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

type testResponseWithCode struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (r *testResponseWithCode) GetCode() int {
	return r.Code
}

func (r *testResponseWithCode) GetMessage() string {
	return r.Message
}

// TestDecodeJSONResponse verifies plain JSON decode behavior.
func TestDecodeJSONResponse(t *testing.T) {
	var out struct {
		Value int `json:"value"`
	}
	require.NoError(t, decodeJSONResponse([]byte(`{"value":7}`), &out))
	require.Equal(t, 7, out.Value)

	err := decodeJSONResponse([]byte(`{"value"`), &out)
	require.Error(t, err)
}

// TestDecodeAndCheckJSONResponse verifies code/message error mapping.
func TestDecodeAndCheckJSONResponse(t *testing.T) {
	ok := &testResponseWithCode{}
	require.NoError(t, decodeAndCheckJSONResponse([]byte(`{"code":0,"message":""}`), ok))

	serverErr := &testResponseWithCode{}
	err := decodeAndCheckJSONResponse([]byte(`{"code":9731,"message":"server failed"}`), serverErr)
	require.Error(t, err)
	var taosErr *taosErrors.TaosError
	require.True(t, errors.As(err, &taosErr))
	require.Equal(t, int32(9731)&0xffff, taosErr.Code)
	require.Equal(t, "server failed", taosErr.ErrStr)

	badJSON := &testResponseWithCode{}
	require.Error(t, decodeAndCheckJSONResponse([]byte(`{"code"`), badJSON))
}

// TestDecodeAndCheckJSONResponseAsProtocol verifies protocol error wrapping on invalid payload.
func TestDecodeAndCheckJSONResponseAsProtocol(t *testing.T) {
	ok := &testResponseWithCode{}
	require.NoError(t, decodeAndCheckJSONResponseAsProtocol([]byte(`{"code":0,"message":""}`), ok, "invalid payload"))

	err := decodeAndCheckJSONResponseAsProtocol([]byte(`{"code"`), &testResponseWithCode{}, "invalid query response")
	require.Error(t, err)
	require.True(t, IsErrorType(err, ErrorTypeProtocol))
	require.Contains(t, err.Error(), "invalid query response")

	serverErr := decodeAndCheckJSONResponseAsProtocol(
		[]byte(`{"code":2603,"message":"query failed"}`),
		&testResponseWithCode{},
		"invalid query response",
	)
	require.Error(t, serverErr)
	var taosErr *taosErrors.TaosError
	require.True(t, errors.As(serverErr, &taosErr))
	require.Equal(t, int32(2603)&0xffff, taosErr.Code)
	require.Equal(t, "query failed", taosErr.ErrStr)
}
