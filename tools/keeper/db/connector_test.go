package db

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/testutil"
	"github.com/taosdata/taoskeeper/util"
)

func TestMain(m *testing.M) {
	config.InitConfig()
	os.Exit(m.Run())
}

func TestIsAuthFailure(t *testing.T) {
	// 0x0357 literal, not authFailureErrCode: the constant is only correct
	// because driver-go masks the module bits off TDengine's wire value
	// (TSDB_CODE_MND_AUTH_FAILURE = 0x80000357 -> NewError does code & 0xffff).
	// Asserting against the constant itself would pass even if it were wrong.
	assert.True(t, isAuthFailure(&taosErrors.TaosError{Code: 0x0357, ErrStr: "Authentication failure"}))
	// attacker-controlled text echoed in a syntax error must not match
	assert.False(t, isAuthFailure(&taosErrors.TaosError{
		Code:   0x2600,
		ErrStr: "syntax error near 'Authentication failure, 'a', 1, 'b') ' (invalid timestamp format)",
	}))
	assert.False(t, isAuthFailure(errors.New("Authentication failure")))
	assert.False(t, isAuthFailure(nil))

	// an intermediary answering 401 with a non-TDengine body yields the
	// driver's untyped "server response: <status> - <body>" error; it is not
	// proof of a credential problem, so it must NOT be treated as one (that
	// would let any proxy in front of taosAdapter restart-loop keeper)
	assert.False(t, isAuthFailure(errors.New("server response: 401 Unauthorized - <html>nginx</html>")))
	assert.False(t, isAuthFailure(errors.New("server response: 400 Bad Request - server response: 401")))
	assert.False(t, isAuthFailure(errors.New(`{"code":401}`)))
	// only the typed TDengine auth code counts, whatever the text says
	assert.True(t, isAuthFailure(&taosErrors.TaosError{Code: 0x0357, ErrStr: "anything"}))
	assert.False(t, isAuthFailure(&taosErrors.TaosError{Code: 0x0358, ErrStr: "Authentication failure"}))
}

func TestIPv6(t *testing.T) {
	conn, err := NewConnector(testutil.TestUsername(), testutil.TestPassword(), "[::1]", 6041, false)
	assert.NoError(t, err)

	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_ipv6", 1001)
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "create database test_ipv6", 1002)
	assert.NoError(t, err)

	conn, err = NewConnectorWithDb(testutil.TestUsername(), testutil.TestPassword(), "[::1]", 6041, "test_ipv6", false)
	assert.NoError(t, err)

	defer conn.Close()

	_, err = conn.Exec(context.Background(), "create table t0(ts timestamp, c1 int)", 1003)
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into t0 values(1726803358466, 1)", 1004)
	assert.NoError(t, err)

	data, err := conn.Query(context.Background(), "select * from t0", 1005)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))
	assert.Equal(t, int64(1726803358466), data.Data[0][0].(time.Time).UnixMilli())
	assert.Equal(t, int32(1), data.Data[0][1])

	_, err = conn.Exec(context.Background(), "drop database test_ipv6", 1006)
	assert.NoError(t, err)
}

// TestLogHook captures log entries for verification
type TestLogHook struct {
	Entries []logrus.Entry
}

func (h *TestLogHook) Fire(entry *logrus.Entry) error {
	h.Entries = append(h.Entries, *entry)
	return nil
}

func (h *TestLogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func TestExecuteWithRetry(t *testing.T) {
	tests := []struct {
		name                string
		maxRetries          int
		baseDelay           time.Duration
		maxDelay            time.Duration
		failures            int           // Number of simulated failures
		expectedAttempts    int           // Expected number of attempts
		expectedFinalDelay  time.Duration // Expected final delay before success
		shouldSucceed       bool          // Should the operation succeed?
		expectedError       error         // Expected error if not successful
		cancelAfterAttempts int           // Cancel context after this many attempts
	}{
		{
			name:               "Succeeds immediately",
			baseDelay:          10 * time.Millisecond,
			maxDelay:           100 * time.Millisecond,
			failures:           0,
			expectedAttempts:   1,
			expectedFinalDelay: 10 * time.Millisecond,
			shouldSucceed:      true,
		},
		{
			name:               "Succeeds after one failure",
			baseDelay:          10 * time.Millisecond,
			maxDelay:           100 * time.Millisecond,
			failures:           1,
			expectedAttempts:   2,
			expectedFinalDelay: 20 * time.Millisecond,
			shouldSucceed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup log capturing
			logger := logrus.New()
			testHook := &TestLogHook{}
			logger.Hooks.Add(testHook)
			logger.Level = logrus.DebugLevel

			// Counters and error simulation
			var attemptCounter int32
			failuresRemaining := tt.failures

			config := RetryConfig{
				BaseDelay: tt.baseDelay,
				MaxDelay:  tt.maxDelay,
				Logger:    logger.WithField("test", tt.name),
			}

			// Simulated operation function
			op := func() (int, error) {
				attempt := int(atomic.AddInt32(&attemptCounter, 1))

				// Simulate failures
				if failuresRemaining > 0 {
					failuresRemaining--
					return 0, errors.New("simulated error")
				}

				return attempt, nil
			}

			startTime := time.Now()
			result, err := executeWithRetry(config, op)
			elapsed := time.Since(startTime)

			// Verify results
			if tt.shouldSucceed {
				if err != nil {
					t.Fatalf("expected success but got error: %v", err)
				}
				if result != tt.expectedAttempts {
					t.Errorf("expected result %d, got %d", tt.expectedAttempts, result)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error but got success")
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			}

			// Verify attempt count
			if int(attemptCounter) != tt.expectedAttempts {
				t.Errorf("expected %d attempts, got %d", tt.expectedAttempts, attemptCounter)
			}
			// Verify delay time
			if len(testHook.Entries) > 1 {
				lastEntry := testHook.Entries[len(testHook.Entries)-2] // Last error log
				delayField, exists := lastEntry.Data["retry_delay"]
				if !exists {
					t.Error("missing retry_delay field in log entry")
				} else if delayField != tt.expectedFinalDelay.String() {
					t.Errorf("expected final delay %v, got %v", tt.expectedFinalDelay, delayField)
				}
			}

			// Verify total execution time
			minExpectedTime := time.Duration(0)
			currentDelay := tt.baseDelay
			for i := 1; i < tt.expectedAttempts; i++ {
				minExpectedTime += currentDelay
				currentDelay = time.Duration(math.Min(float64(currentDelay)*2, float64(tt.maxDelay)))
			}

			if elapsed < minExpectedTime {
				t.Errorf("execution time %v is shorter than expected minimum %v", elapsed, minExpectedTime)
			}
		})
	}
}

func newTestLogger(buf *bytes.Buffer, level logrus.Level) *logrus.Entry {
	lg := logrus.New()
	lg.SetOutput(buf)
	lg.SetLevel(level)
	lg.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})
	return logrus.NewEntry(lg)
}

func Test_logData_NilData_TracesNoData(t *testing.T) {
	var buf bytes.Buffer
	entry := newTestLogger(&buf, logrus.TraceLevel)

	logData(nil, entry)

	got := buf.String()
	assert.Contains(t, got, "No data to display")
}

func Test_logData_MarshalError_LogsError(t *testing.T) {
	var buf bytes.Buffer
	entry := newTestLogger(&buf, logrus.TraceLevel)

	bad := &Data{
		Head: []string{"col"},
		Data: [][]interface{}{{func() {}}},
	}
	logData(bad, entry)

	got := buf.String()
	if !strings.Contains(got, "Failed to marshal data to JSON") {
		t.Fatalf("expected error log about marshal failure, got: %q", got)
	}
}

func Test_logData_Success_LogsJSONTrace(t *testing.T) {
	var buf bytes.Buffer
	entry := newTestLogger(&buf, logrus.TraceLevel)

	ok := &Data{
		Head: []string{"h"},
		Data: [][]interface{}{{1, "x"}},
	}
	logData(ok, entry)

	got := buf.String()
	if !strings.Contains(got, "query result data:") {
		t.Fatalf("expected trace with 'query result data:', got: %q", got)
	}
}

type errDriver struct{}

func (d errDriver) Open(name string) (driver.Conn, error) { return &errConn{}, nil }

type errConn struct{}

func (c *errConn) Prepare(query string) (driver.Stmt, error) { return &noopStmt{}, nil }
func (c *errConn) Close() error                              { return nil }
func (c *errConn) Begin() (driver.Tx, error)                 { return nil, errors.New("no tx") }
func (c *errConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("boom: some query error")
}

type noopStmt struct{}

func (s *noopStmt) Close() error  { return nil }
func (s *noopStmt) NumInput() int { return -1 }
func (s *noopStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not supported")
}
func (s *noopStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("not supported")
}

func TestConnectorQuery_ErrorPath_NoAuthExit_ReturnsError(t *testing.T) {
	const drv = "errdrv_query"
	sql.Register(drv, errDriver{})
	dbh, err := sql.Open(drv, "")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	defer dbh.Close()
	dbh.SetConnMaxLifetime(time.Second)

	c := &Connector{db: dbh}

	data, qerr := c.Query(context.Background(), "SELECT 1", 123)
	if qerr == nil {
		t.Fatalf("expected error, got nil")
	}
	if data != nil {
		t.Fatalf("expected nil data on error, got %#v", data)
	}
	if qerr.Error() == "Authentication failure" {
		t.Fatalf("unexpected auth failure branch triggered")
	}
}

func IsEnterpriseTest() bool {
	if _, ok := os.LookupEnv("TEST_ENTERPRISE"); ok {
		return true
	}
	return false
}

func TestNewConnectorWithDbAndToken(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	conn, err := NewConnector(testutil.TestUsername(), testutil.TestPassword(), "localhost", 6041, false)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766988529", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766988529", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	conn.Exec(context.Background(), "drop user c_token_user", util.GetQidOwn(config.Conf.InstanceID))
	_, err = conn.Exec(context.Background(), "create user c_token_user pass 'token_pass_1'", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766988529 to c_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766988529.* to c_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	data, err := conn.Query(context.Background(), "create token test_c_bearer_token from user c_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	conn1, err := NewConnectorWithDbAndToken("", "", token, "localhost", 6041, "test_1766988529", false)
	assert.NoError(t, err)
	defer conn1.Close()

	conn2, err := NewConnectorWithDbAndToken(testutil.TestUsername(), testutil.TestPassword(), token, "localhost", 6041, "test_1766988529", false)
	assert.NoError(t, err)
	defer conn2.Close()

	_, err = conn2.Exec(context.Background(), "create stable st (ts timestamp, c1 int) tags (t1 int)", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766988529", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "drop user c_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
}

func TestNewConnectorWithDbAndTokenWithSpecialChars(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	conn, err := NewConnector(testutil.TestUsername(), testutil.TestPassword(), "localhost", 6041, false)
	assert.NoError(t, err)
	defer conn.Close()

	conn.Exec(context.Background(), "drop user user_1768380767", util.GetQidOwn(config.Conf.InstanceID))
	_, err = conn.Exec(context.Background(), "create user user_1768380767 pass 'AaZz01234!@#$%^&*()-_+=[]{}:;><?|~,.'", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	data, err := conn.Query(context.Background(), "create token token_1768380767 from user user_1768380767", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	conn1, err := NewConnectorWithDbAndToken("", "", token, "localhost", 6041, "test_1766988529", false)
	assert.NoError(t, err)
	defer conn1.Close()

	_, err = conn1.Query(context.Background(), "select server_version()", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	conn2, err := NewConnectorWithDbAndToken("user_1768380767", "AaZz01234!@#$%^&*()-_+=[]{}:;><?|~,.", token, "localhost", 6041, "test_1766988529", false)
	assert.NoError(t, err)
	defer conn2.Close()

	_, err = conn2.Query(context.Background(), "select server_version()", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	conn3, err := NewConnectorWithDbAndToken("user_1768380767", "AaZz01234!@#$%^&*()-_+=[]{}:;><?|~,.", "", "localhost", 6041, "test_1766988529", false)
	assert.NoError(t, err)
	defer conn3.Close()

	_, err = conn3.Query(context.Background(), "select server_version()", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user user_1768380767", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
}

func TestNewConnectorWithSpecialChars(t *testing.T) {
	conn, err := NewConnector(testutil.TestUsername(), testutil.TestPassword(), "localhost", 6041, false)
	assert.NoError(t, err)
	defer conn.Close()

	conn.Exec(context.Background(), "drop user user_1768379499", util.GetQidOwn(config.Conf.InstanceID))
	_, err = conn.Exec(context.Background(), "create user user_1768379499 pass 'AaZz01234!@#$%^&*()-_+=[]{}:;><?|~,.'", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	conn1, err := NewConnector("user_1768379499", "AaZz01234!@#$%^&*()-_+=[]{}:;><?|~,.", "localhost", 6041, false)
	assert.NoError(t, err)
	defer conn1.Close()

	_, err = conn1.Query(context.Background(), "select server_version()", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user user_1768379499", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
}

func TestBuildRESTfulDSNToken(t *testing.T) {
	// a valid token is sent verbatim: the driver does not unescape the DSN
	// bearerToken parameter, so escaping it here would send the escaped form
	dsn := buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, "audit", "abc.def_123-xyz")
	assert.Contains(t, dsn, "&bearerToken=abc.def_123-xyz")
	// only the two intended query parameters survive
	assert.Equal(t, 1, strings.Count(dsn, "&"))

	// empty token adds no parameter
	dsn = buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, "audit", "")
	assert.NotContains(t, dsn, "bearerToken")
}

func TestValidateBearerToken(t *testing.T) {
	assert.NoError(t, validateBearerToken(""))
	assert.NoError(t, validateBearerToken("ABmTXHdQAN9au7w4JcXcp4gpXgrDxLxhjaIOiumuA8f1"))
	// the driver splits a parameter on the first '=' only, so a padded token
	// keeps its full value and must not be rejected
	assert.NoError(t, validateBearerToken("AbC12xY="))

	// '&' appends driver parameters (e.g. skipVerify), '/' moves the boundary
	// the driver uses to find the database name, and whitespace would be
	// written into the Authorization header
	assert.Error(t, validateBearerToken("x&skipVerify=true"))
	assert.Error(t, validateBearerToken("a/b"))
	assert.Error(t, validateBearerToken("a b"))
	assert.Error(t, validateBearerToken("a\tb"))
	assert.Error(t, validateBearerToken("a\nb"))
}

// skipVerify comes from configuration — the only place an operator can turn
// certificate verification off (finding 301) — not from a per-call argument.
func TestBuildRESTfulDSNSkipVerifyFromConfig(t *testing.T) {
	old := config.Conf.TDengine.SkipVerify
	t.Cleanup(func() { config.Conf.TDengine.SkipVerify = old })

	config.Conf.TDengine.SkipVerify = true
	assert.Contains(t, buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, "log", ""), "skipVerify=true")

	config.Conf.TDengine.SkipVerify = false
	assert.Contains(t, buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, "log", ""), "skipVerify=false")
}

// The database name must reach the driver verbatim. The driver builds a
// url.URL from the DSN path and escapes it once, so a name escaped here would
// be escaped a second time — "metrics db" would arrive at the server as
// "metrics%2520db" and every write would target a database that does not exist.
func TestBuildRESTfulDSNPassesDbNameVerbatim(t *testing.T) {
	old := config.Conf.TDengine.SkipVerify
	t.Cleanup(func() { config.Conf.TDengine.SkipVerify = old })
	config.Conf.TDengine.SkipVerify = false

	dsn := buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, "log", "")
	assert.Contains(t, dsn, "/log?skipVerify=false")

	for _, name := range []string{"metrics db", "数据", "a%b"} {
		dsn = buildRESTfulDSN("root", "taosdata", "http", "127.0.0.1", 6041, name, "")
		assert.Contains(t, dsn, "/"+name+"?skipVerify=false", "the name must not be pre-escaped")
	}
}

// A '?' cannot be allowed through in a database name: the driver takes
// everything after the last '/' up to the first '?' as the name, so a '?' ends
// the name early and turns the remainder into DSN parameters, dropping
// skipVerify and the token along with it. Rejecting gives the caller a clear
// error instead of a silently different connection.
func TestValidateDBName(t *testing.T) {
	assert.NoError(t, validateDBName(""))
	assert.NoError(t, validateDBName("log"))
	assert.NoError(t, validateDBName("metrics db"))
	assert.NoError(t, validateDBName("数据"))

	assert.Error(t, validateDBName("log?skipVerify=true"))
	assert.Error(t, validateDBName("a?b"))
}
