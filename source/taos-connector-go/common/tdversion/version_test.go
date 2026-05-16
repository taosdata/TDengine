package tdversion

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestParseVersion(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name    string
		args    args
		want    *Version
		wantErr bool
	}{
		{
			name: "valid version",
			args: args{
				v: "3.3.6",
			},
			wantErr: true,
		},
		{
			name: "3.3.6.0",
			args: args{
				v: "3.3.6.0",
			},
			want:    &Version{3, 3, 6, 0},
			wantErr: false,
		},
		{
			name: "3.3.6.0.alpha",
			args: args{
				v: "3.3.6.0.alpha",
			},
			want:    &Version{3, 3, 6, 0},
			wantErr: false,
		},
		{
			name: "a.3.6.0",
			args: args{
				v: "a.3.6.0",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersion(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckVersionCompatibility(t *testing.T) {
	type args struct {
		currentVersion string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "compatible versions",
			args: args{
				currentVersion: "3.3.6.0",
			},
			wantErr: false,
		},
		{
			name: "incompatible versions",
			args: args{
				currentVersion: "3.3.5.0",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CheckVersionCompatibility(tt.args.currentVersion); (err != nil) != tt.wantErr {
				t.Errorf("CheckVersionCompatibility() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestVersionMismatchError_Error(t *testing.T) {
	err := &VersionMismatchError{
		CurrentVersion: "3.3.5.0",
		MinimumVersion: "3.3.6.0",
	}
	want := "Version mismatch. The minimum required TDengine version is 3.3.6.0."
	if got := err.Error(); got != want {
		t.Errorf("VersionMismatchError.Error() = %v, want %v", got, want)
	}
}

func TestUnknownVersionError_Error(t *testing.T) {
	err := &UnknownVersionError{
		Version: "invalid.version",
	}
	want := "Unknown TDengine version: invalid.version."
	if got := err.Error(); got != want {
		t.Errorf("UnknownVersionError.Error() = %v, want %v", got, want)
	}
}

// MockWebSocketConn is a mock implementation of websocket.Conn
type MockWebSocketConn struct {
	mock.Mock
}

func (m *MockWebSocketConn) WriteMessage(messageType int, data []byte) error {
	args := m.Called(messageType, data)
	return args.Error(0)
}

func (m *MockWebSocketConn) ReadMessage() (int, []byte, error) {
	args := m.Called()
	return args.Int(0), args.Get(1).([]byte), args.Error(2)
}

func TestWSCheckVersion(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockWebSocketConn)
		expectedError string
	}{
		{
			name: "successful version check",
			setupMock: func(m *MockWebSocketConn) {
				// Expect WriteMessage call
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)

				// Prepare successful response
				resp := VersionResp{
					Code:    0,
					Action:  "version",
					Version: "3.3.6.0",
					Message: "",
				}
				respBytes, _ := json.Marshal(resp)

				// Expect ReadMessage call
				m.On("ReadMessage").Return(websocket.TextMessage, respBytes, nil)
			},
			expectedError: "",
		},
		{
			name: "write message error",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(fmt.Errorf("write error"))
			},
			expectedError: "write error",
		},
		{
			name: "read message error",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)
				m.On("ReadMessage").Return(0, []byte(nil), fmt.Errorf("read error"))
			},
			expectedError: "read error",
		},
		{
			name: "wrong message type",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)
				m.On("ReadMessage").Return(websocket.BinaryMessage, []byte("binary data"), nil)
			},
			expectedError: "get version: response got wrong message type 2, message:binary data",
		},
		{
			name: "invalid json response",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)
				m.On("ReadMessage").Return(websocket.TextMessage, []byte("invalid json"), nil)
			},
			expectedError: "get version: unmarshal json error",
		},
		{
			name: "non-zero response code",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)

				resp := VersionResp{
					Code:    1,
					Action:  "version",
					Version: "1.0.0",
					Message: "Error",
				}
				respBytes, _ := json.Marshal(resp)

				m.On("ReadMessage").Return(websocket.TextMessage, respBytes, nil)
			},
			expectedError: "Error",
		},
		{
			name: "unexpected action",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)

				resp := VersionResp{
					Code:    0,
					Action:  "wrong_action",
					Version: "1.0.0",
					Message: "OK",
				}
				respBytes, _ := json.Marshal(resp)

				m.On("ReadMessage").Return(websocket.TextMessage, respBytes, nil)
			},
			expectedError: "unexpected action: wrong_action",
		},
		{
			name: "version compatibility error",
			setupMock: func(m *MockWebSocketConn) {
				m.On("WriteMessage", websocket.TextMessage, versionReq).Return(nil)

				resp := VersionResp{
					Code:    0,
					Action:  "version",
					Version: "incompatible_version",
					Message: "OK",
				}
				respBytes, _ := json.Marshal(resp)

				m.On("ReadMessage").Return(websocket.TextMessage, respBytes, nil)
			},
			expectedError: "Unknown TDengine version: incompatible_version.", // Adjust based on your CheckVersionCompatibility implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConn := new(MockWebSocketConn)
			tt.setupMock(mockConn)

			err := WSCheckVersion(mockConn)

			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.expectedError)
			}

			mockConn.AssertExpectations(t)
		})
	}
}
