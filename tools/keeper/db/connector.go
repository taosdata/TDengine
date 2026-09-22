package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

type Connector struct {
	db *sql.DB
}

type Data struct {
	Head []string        `json:"head"`
	Data [][]interface{} `json:"data"`
}

var dbLogger = log.GetLogger("DB ")

// buildRESTfulDSN builds the taosRestful DSN, which is the only record of what
// this connection trusts. Username and password are query-escaped because the
// driver runs them through url.QueryUnescape when it parses the DSN.
//
// dbname and token are passed verbatim. The driver does not unescape either:
// it takes the database name as the text after the last '/' in the DSN and
// builds a url.URL out of it, so the URL escapes the name exactly once. An
// escape applied here would be escaped a second time and reach the server as a
// different name ("metrics db" would arrive as "metrics%2520db"), so these are
// guarded by validateDBName and validateBearerToken instead.
//
// skipVerify comes from configuration — the only place an operator can turn
// certificate verification off.
func buildRESTfulDSN(username, password, protocol, host string, port int, dbname string, token string) string {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?skipVerify=%t",
		url.QueryEscape(username), url.QueryEscape(password), protocol, host, port, dbname,
		config.Conf.TDengine.SkipVerify)
	if token != "" {
		dsn += "&bearerToken=" + token
	}
	return dsn
}

func NewConnector(username, password, host string, port int, usessl bool) (*Connector, error) {
	var protocol string
	if usessl {
		protocol = "https"
	} else {
		protocol = "http"
	}
	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn(config.Conf.InstanceID)})
	dbLogger.Tracef("connect to adapter, host:%s, port:%d, usessl:%v", host, port, usessl)

	db, err := sql.Open("taosRestful", buildRESTfulDSN(username, password, protocol, host, port, "", ""))
	if err != nil {
		dbLogger.Errorf("connect to adapter failed, host:%s, port:%d, usessl:%v, error:%s", host, port, usessl, err)
		return nil, err
	}

	dbLogger.Tracef("connect to adapter success, host:%s, port:%d, usessl:%v", host, port, usessl)
	return &Connector{db: db}, nil
}

func NewConnectorWithDb(username, password, host string, port int, dbname string, usessl bool) (*Connector, error) {
	return NewConnectorWithDbAndToken(username, password, "", host, port, dbname, usessl)
}

// validateDBName rejects a database name that would change how the DSN parses.
// The driver scans for the last '/' in the whole DSN and then takes everything
// up to the first '?' as the database name, so a '?' in the name ends the name
// early and turns the remainder into DSN parameters — dropping skipVerify and
// the token along with it. Nothing else is restricted: the driver escapes the
// path itself, so spaces and non-ASCII names arrive intact.
func validateDBName(dbname string) error {
	if strings.ContainsRune(dbname, '?') {
		return fmt.Errorf("invalid database name %q: must not contain '?'", dbname)
	}
	return nil
}

// validateBearerToken rejects a token that would change the DSN query string or
// the Authorization header it is written into. The driver splits the query on
// '&', so an unescaped one could append driver parameters (skipVerify among
// them); it also scans for the last '/' in the whole DSN, so a '/' in the token
// would move that boundary and silently drop every parameter. Whitespace is
// rejected because the token is written into a header. '=' is allowed: the
// driver splits a parameter on the first '=' only, so a padded token keeps its
// full value.
func validateBearerToken(token string) error {
	if token == "" {
		return nil
	}
	if strings.ContainsAny(token, "&/ \t\r\n") {
		return fmt.Errorf("invalid bearer token: must not contain '&', '/' or whitespace")
	}
	return nil
}

func NewConnectorWithDbAndToken(username, password, token, host string, port int, dbname string, usessl bool) (*Connector, error) {
	if err := validateBearerToken(token); err != nil {
		return nil, err
	}
	if err := validateDBName(dbname); err != nil {
		return nil, err
	}

	var protocol string
	if usessl {
		protocol = "https"
	} else {
		protocol = "http"
	}

	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn(config.Conf.InstanceID)})
	dbLogger.Tracef("connect to adapter, host:%s, port:%d, usessl:%v", host, port, usessl)

	db, err := sql.Open("taosRestful", buildRESTfulDSN(username, password, protocol, host, port, dbname, token))
	if err != nil {
		dbLogger.Errorf("connect to adapter failed, host:%s, port:%d, db:%s, usessl:%v, error:%s", host, port, dbname, usessl, err)
		return nil, err
	}

	dbLogger.Tracef("connect to adapter success, host:%s, port:%d, db:%s, usessl:%v", host, port, dbname, usessl)
	return &Connector{db: db}, nil
}

type ReqIDKeyTy string

const ReqIDKey ReqIDKeyTy = "taos_req_id"

// TSDB_CODE_MND_AUTH_FAILURE, the only TDengine error whose message is "Authentication failure"
const authFailureErrCode int32 = 0x0357

// isAuthFailure reports whether err is a TDengine authentication failure,
// identified by its typed error code. The error text is deliberately not
// matched: TDengine echoes the submitted SQL back in its syntax errors, so
// matching text let any network peer terminate keeper with a crafted request.
//
// An untyped HTTP 401 is not an authentication failure either. The driver
// returns "server response: 401 Unauthorized - ..." when something other than
// taosAdapter answers with a non-TDengine body (an ingress, a WAF, a connection
// pointed at the wrong port); exiting on that would turn any such intermediary
// into a restart loop. It is reported as an ordinary error instead.
func isAuthFailure(err error) bool {
	var taosErr *taosErrors.TaosError
	return errors.As(err, &taosErr) && taosErr.Code == authFailureErrCode
}

type RetryConfig struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Logger    *logrus.Entry
}

var defaultRetryConfig = RetryConfig{
	BaseDelay: 5 * time.Second,
	MaxDelay:  60 * time.Second,
}

type operationWithResult[T any] func() (T, error)

func (c *Connector) Exec(ctx context.Context, sql string, qid uint64) (int64, error) {
	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: qid})
	ctx = context.WithValue(ctx, ReqIDKey, int64(qid))

	dbLogger.Tracef("call adapter to execute sql:%s", sql)
	startTime := time.Now()
	res, err := c.db.ExecContext(ctx, sql)

	endTime := time.Now()
	latency := endTime.Sub(startTime)

	if err != nil {
		if isAuthFailure(err) {
			dbLogger.Error("Authentication failure")
			ctxLog, cancelLog := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelLog()
			log.Close(ctxLog)
			os.Exit(1)
		}
		dbLogger.Errorf("latency:%v, sql:%s, err:%s", latency, sql, err)
		return 0, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		dbLogger.Errorf("latency:%v, err:%s", latency, err)
		return rowsAffected, err
	}

	dbLogger.Tracef("response ok, rowsAffected:%v, latency:%v", rowsAffected, latency)

	return rowsAffected, err
}

func logData(data *Data, logger *logrus.Entry) {
	if data == nil {
		logger.Tracef("No data to display")
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Errorf("Failed to marshal data to JSON: %v", err)
		return
	}
	logger.Tracef("query result data:%s", jsonData)
}

func (c *Connector) Query(ctx context.Context, sql string, qid uint64) (*Data, error) {
	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: qid})
	ctx = context.WithValue(ctx, ReqIDKey, int64(qid))

	dbLogger.Tracef("call adapter to execute query, sql:%s", sql)

	startTime := time.Now()
	rows, err := c.db.QueryContext(ctx, sql)

	endTime := time.Now()
	latency := endTime.Sub(startTime)

	if err != nil {
		if isAuthFailure(err) {
			dbLogger.Error("Authentication failure")
			ctxLog, cancelLog := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelLog()
			log.Close(ctxLog)
			os.Exit(1)
		}
		dbLogger.Errorf("latency:%v, sql:%s, err:%s", latency, sql, err)
		return nil, err
	}

	dbLogger.Tracef("response ok, latency:%v, sql:%s", latency, sql)

	data := &Data{}
	data.Head, err = rows.Columns()
	columnCount := len(data.Head)
	if err != nil {
		dbLogger.Errorf("get columns error, msg:%s", err)
		return nil, err
	}
	scanData := make([]interface{}, columnCount)
	for rows.Next() {
		tmp := make([]interface{}, columnCount)
		for i := 0; i < columnCount; i++ {
			scanData[i] = &tmp[i]
		}
		err = rows.Scan(scanData...)
		if err != nil {
			rows.Close()
			dbLogger.Errorf("rows scan error, msg:%s", err)
			return nil, err
		}
		data.Data = append(data.Data, tmp)
	}

	if dbLogger.Logger.IsLevelEnabled(logrus.TraceLevel) {
		logData(data, dbLogger)
	}
	return data, nil
}

func (c *Connector) Close() error {
	return c.db.Close()
}

func executeWithRetry[T any](config RetryConfig, op operationWithResult[T]) (T, error) {
	var (
		delay   = config.BaseDelay
		attempt = 0
	)

	for {
		result, err := op()
		if err == nil {
			return result, nil
		}

		attempt++
		config.Logger.Errorf("Attempt %d failed, error: %v, retrying in %v...",
			attempt, err, delay)

		time.Sleep(delay)
		delay = time.Duration(math.Min(float64(delay)*2, float64(config.MaxDelay)))
	}
}

func (c *Connector) ExecWithRetryForever(ctx context.Context, sql string, qid uint64) (int64, error) {
	logger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: qid})
	config := defaultRetryConfig
	config.Logger = logger

	return executeWithRetry(config, func() (int64, error) {
		return c.Exec(ctx, sql, qid)
	})
}

func (c *Connector) QueryWithRetryForever(ctx context.Context, sql string, qid uint64) (*Data, error) {
	logger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: qid})
	config := defaultRetryConfig
	config.Logger = logger

	return executeWithRetry(config, func() (*Data, error) {
		return c.Query(ctx, sql, qid)
	})
}

func NewConnectorWithRetryForever(username, password, host string, port int, usessl bool) (*Connector, error) {
	logger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn(config.Conf.InstanceID)})
	config := defaultRetryConfig
	config.Logger = logger

	return executeWithRetry(config, func() (*Connector, error) {
		return NewConnector(username, password, host, port, usessl)
	})
}

func NewConnectorWithDbWithRetryForever(username, password, host string, port int, dbname string, usessl bool) (*Connector, error) {
	logger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn(config.Conf.InstanceID)})
	config := defaultRetryConfig
	config.Logger = logger
	return executeWithRetry(config, func() (*Connector, error) {
		return NewConnectorWithDb(username, password, host, port, dbname, usessl)
	})
}

func NewConnectorWithDbAndTokenWithRetryForever(username, password, token, host string, port int, dbname string, usessl bool) (*Connector, error) {
	// validate before entering the retry loop: a malformed token or database
	// name never becomes valid, and executeWithRetry would otherwise retry it
	// forever
	if err := validateBearerToken(token); err != nil {
		return nil, err
	}
	if err := validateDBName(dbname); err != nil {
		return nil, err
	}

	logger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn(config.Conf.InstanceID)})
	config := defaultRetryConfig
	config.Logger = logger
	return executeWithRetry(config, func() (*Connector, error) {
		return NewConnectorWithDbAndToken(username, password, token, host, port, dbname, usessl)
	})
}
