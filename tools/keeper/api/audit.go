// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

var auditLogger = log.GetLogger("AUD")

const MAX_DETAIL_LEN = 50000

// maxEscapedDetailLen is the parser's ceiling for one string token
// (TSDB_MAX_BYTES_PER_ROW in tdef.h). TDengine refuses a string token at or
// above it before decoding any escapes, so it bounds the escaped literal that
// handleDetails builds, not the column width.
const maxEscapedDetailLen = 65531

type Audit struct {
	username  string
	password  string
	host      string
	port      int
	usessl    bool
	conn      *db.Connector
	db        string
	dbOptions map[string]interface{}
	token     string

	inited bool
	mu     sync.Mutex
}

type AuditInfo struct {
	Timestamp    string  `json:"timestamp"`
	ClusterID    string  `json:"cluster_id"`
	User         string  `json:"user"`
	Operation    string  `json:"operation"`
	Db           string  `json:"db"`
	Resource     string  `json:"resource"`
	ClientAdd    string  `json:"client_add"` // client address
	Details      string  `json:"details"`
	AffectedRows uint64  `json:"affected_rows"`
	Duration     float64 `json:"duration"`
}

type AuditArrayInfo struct {
	Records []AuditInfo `json:"records"`
}

type AuditInfoOld struct {
	Timestamp int64  `json:"timestamp"`
	ClusterID string `json:"cluster_id"`
	User      string `json:"user"`
	Operation string `json:"operation"`
	Db        string `json:"db"`
	Resource  string `json:"resource"`
	ClientAdd string `json:"client_add"` // client address
	Details   string `json:"details"`
}

func NewAudit(cfg *config.Config) *Audit {
	audit := Audit{
		username:  cfg.TDengine.Username,
		password:  cfg.TDengine.Password,
		host:      cfg.TDengine.Host,
		port:      cfg.TDengine.Port,
		usessl:    cfg.TDengine.Usessl,
		db:        getAuditDBName(cfg),
		dbOptions: cfg.Audit.Database.Options,
	}
	return &audit
}

func getAuditDBName(cfg *config.Config) string {
	name := cfg.Audit.Database.Name
	if name == "" {
		return "audit"
	}
	return name
}

func (a *Audit) Init(c gin.IRouter) {
	c.POST("/audit", a.handleFunc())
	c.POST("/audit_v2", a.handleFunc())
	c.POST("/audit-batch", a.handleBatchFunc())
}

func (a *Audit) handleBatchFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))
		auditLogger := auditLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		if !a.prepareConnectionAndTable(c, auditLogger) {
			return
		}
		if a.conn == nil {
			auditLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			auditLogger.Errorf("get audit data error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get audit data error. %s", err)})
			return
		}

		if auditLogger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			auditLogger.Tracef("receive audit request, data:%s", string(data))
		}

		var auditArray AuditArrayInfo
		if err := json.Unmarshal(data, &auditArray); err != nil {
			auditLogger.Errorf("parse audit data error, data:%s, error:%s", string(data), err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse audit data error: %s", err)})
			return
		}

		if len(auditArray.Records) == 0 {
			if auditLogger.Logger.IsLevelEnabled(logrus.TraceLevel) {
				auditLogger.Trace("handle request successfully (no records)")
			}
			c.JSON(http.StatusOK, gin.H{})
			return
		}

		if err := handleBatchRecord(auditArray.Records, a.conn, qid); err != nil {
			auditLogger.Errorf("process records error, error:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("process records error. %s", err)})
			return
		}

		c.JSON(http.StatusOK, gin.H{})
	}
}

func (a *Audit) handleFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))
		auditLogger := auditLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		if !a.prepareConnectionAndTable(c, auditLogger) {
			return
		}
		if a.conn == nil {
			auditLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			auditLogger.Errorf("get audit data error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get audit data error. %s", err)})
			return
		}
		if auditLogger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			auditLogger.Tracef("receive audit request, data:%s", string(data))
		}
		sql := ""

		isStrTime, _ := regexp.MatchString(`"timestamp"\s*:\s*"[^"]*"`, string(data))
		if isStrTime {
			var audit AuditInfo
			if err := json.Unmarshal(data, &audit); err != nil {
				auditLogger.Errorf("parse audit data error, data:%s, error:%s", string(data), err)
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse audit data error: %s", err)})
				return
			}

			sql, err = parseSql(audit)
			if err != nil {
				auditLogger.Errorf("invalid audit data, error:%s", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid audit data error: %s", err)})
				return
			}
		} else {
			var audit AuditInfoOld
			if err := json.Unmarshal(data, &audit); err != nil {
				auditLogger.Errorf("parse old audit error, data:%s, error:%s", string(data), err)
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse audit data error: %s", err)})
				return
			}

			sql = parseSqlOld(audit)
		}

		if _, err = a.conn.Exec(context.Background(), sql, qid); err != nil {
			auditLogger.Errorf("save audit data error, sql:%s, error:%s", sql, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("save audit data error: %s", err)})
			return
		}
		c.JSON(http.StatusOK, gin.H{})
	}
}

// validAuditDBName matches plain TDengine database names. The db query
// parameter ends up in the connection DSN; rejecting anything beyond a plain
// name here fails the request with a clear 400 instead of a confusing
// connect error.
var validAuditDBName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (a *Audit) prepareConnectionAndTable(c *gin.Context, logger *logrus.Entry) bool {
	db := c.Query("db")
	token := c.Query("token")

	isDefault := db == ""
	if isDefault {
		db = getAuditDBName(config.Conf)
	} else if !validAuditDBName.MatchString(db) {
		logger.Errorf("invalid audit db name: %q", db)
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid db name"})
		return false
	}

	logger.Tracef("prepare audit connection for db: %s, token is empty: %v", db, token == "")

	if !a.inited || db != a.db || token != a.token {
		a.mu.Lock()
		defer a.mu.Unlock()
		if !a.inited || db != a.db || token != a.token {
			logger.Infof("create audit connection for db: %s", db)

			// Mark the cache unusable before rebuilding. The (db, token) pair
			// is what the guard above treats as "what a.conn is", so without
			// this a failed switch would look like a cache hit on the next
			// identical request, which would then be written through the
			// previous connection, into the previous database.
			//
			// The previous connector is deliberately left open rather than
			// closed here: a concurrent request may already have read it into
			// a local (the handlers do not hold a.mu while they write), and
			// closing it would fail that request or hand it a nil. inited==false
			// is what keeps it from being handed out again — it is only set
			// back to true once a build for this key has succeeded.
			a.inited = false
			a.db = db
			a.token = token

			if isDefault {
				if err := a.createDatabase(); err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("create database error: %s", err)})
					return false
				}
			}

			if err := a.createConnect(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("create connect error: %s", err)})
				return false
			}
			if err := a.createSTable(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("create stable error: %s", err)})
				return false
			}
			a.inited = true
		}
	}
	return true
}

// handleDetails truncates details to the column width and escapes it for a
// single-quoted SQL string literal. TDengine checks the column width against
// the unescaped value, so truncating before escaping keeps the stored value
// within the varchar(50000) column.
func handleDetails(details string) string {
	out := util.EscapeTrunc(details, MAX_DETAIL_LEN)
	if len(out) < maxEscapedDetailLen {
		return out
	}
	// Escaping doubles a backslash or a quote, so details dominated by those
	// characters can grow past the parser's per-token limit even though the
	// raw text fits the column. Clamp the input to half the limit — which
	// escaping can never exceed — rather than let the whole insert be refused.
	return util.EscapeTrunc(details, maxEscapedDetailLen/2)
}

// normalizeAuditTimestamp validates the v2 audit timestamp and returns its
// canonical form. The value lands in a TIMESTAMP column (int64 microseconds),
// so reject anything beyond math.MaxInt64 instead of failing at insert time.
// Normalizing matters: ParseUint accepts leading zeros, which a C-based SQL
// parser could read as octal, so the canonical form is what must reach the
// statement.
func normalizeAuditTimestamp(ts string) (string, error) {
	v, err := strconv.ParseUint(ts, 10, 64)
	if err != nil || v > math.MaxInt64 {
		return "", fmt.Errorf("invalid timestamp %q", ts)
	}
	return strconv.FormatUint(v, 10), nil
}

// buildAuditValues builds one values tuple for the v2 audit format. All
// request-controlled strings are truncated to their column widths
// (user_name 25, operation 20, db 65, resource 193, client_address 64) and
// escaped. The timestamp is normalized here so no caller can concatenate an
// unchecked value into SQL.
func buildAuditValues(audit AuditInfo) (string, error) {
	normalizedTs, err := normalizeAuditTimestamp(audit.Timestamp)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"(%s, '%s', '%s', '%s', '%s', '%s', '%s', %d, %f)",
		normalizedTs,
		util.EscapeTrunc(audit.User, 25),
		util.EscapeTrunc(audit.Operation, 20),
		util.EscapeTrunc(audit.Db, 65),
		util.EscapeTrunc(audit.Resource, 193),
		util.EscapeTrunc(audit.ClientAdd, 64),
		handleDetails(audit.Details), audit.AffectedRows, audit.Duration), nil
}

// parseSql builds the insert for the v2 audit format. All request-controlled
// strings are truncated to their column widths (user_name 25, operation 20,
// db 65, resource 193, client_address 64, cluster_id tag 64) and escaped.
func parseSql(audit AuditInfo) (string, error) {
	values, err := buildAuditValues(audit)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values %s",
		getTableName(audit), util.EscapeTrunc(audit.ClusterID, 64),
		values), nil
}

func parseSqlOld(audit AuditInfoOld) string {
	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values (%s, '%s', '%s', '%s', '%s', '%s', '%s', 0, 0)",
		getTableNameOld(audit), util.EscapeTrunc(audit.ClusterID, 64),
		strconv.FormatInt(audit.Timestamp, 10)+"000000",
		util.EscapeTrunc(audit.User, 25),
		util.EscapeTrunc(audit.Operation, 20),
		util.EscapeTrunc(audit.Db, 65),
		util.EscapeTrunc(audit.Resource, 193),
		util.EscapeTrunc(audit.ClientAdd, 64),
		handleDetails(audit.Details))
}

func handleBatchRecord(auditArray []AuditInfo, conn *db.Connector, qid uint64) error {
	// Validate every record before building any SQL: buildAuditValues fails only
	// on a timestamp that is not a valid int64 (see normalizeAuditTimestamp), so
	// validating up front is enough to keep a bad record from rejecting the
	// request after an earlier statement was already executed. The tuples are
	// rebuilt in the flush loop below rather than held as a slice: this endpoint
	// has no request body limit, so retaining every built tuple would make peak
	// memory a multiple of an unbounded request body.
	for _, audit := range auditArray {
		if _, err := normalizeAuditTimestamp(audit.Timestamp); err != nil {
			return err
		}
	}

	var builder strings.Builder
	var head = fmt.Sprintf(
		"insert into %s using operations tags ('%s') values",
		getTableName(auditArray[0]), util.EscapeTrunc(auditArray[0].ClusterID, 64))

	builder.WriteString(head)
	var qid_counter uint8 = 0
	for _, audit := range auditArray {
		// cannot fail: every timestamp was validated above
		values, err := buildAuditValues(audit)
		if err != nil {
			return err
		}
		valuesStr := values + " "

		if (builder.Len() + len(valuesStr)) > MAX_SQL_LEN {
			sql := builder.String()
			if _, err := conn.Exec(context.Background(), sql, qid|uint64((qid_counter%255))); err != nil {
				return err
			}
			builder.Reset()
			builder.WriteString(head)
		}
		builder.WriteString(valuesStr)
		qid_counter++
	}

	if builder.Len() > len(head) {
		sql := builder.String()
		if _, err := conn.Exec(context.Background(), sql, qid|uint64((qid_counter%255))); err != nil {
			return err
		}
	}

	return nil
}

func getTableName(audit AuditInfo) string {
	return fmt.Sprintf("t_operations_%s", util.ToValidTableName(audit.ClusterID))
}

func getTableNameOld(audit AuditInfoOld) string {
	return fmt.Sprintf("t_operations_%s", util.ToValidTableName(audit.ClusterID))
}

func (a *Audit) createConnect() error {
	conn, err := db.NewConnectorWithDbAndTokenWithRetryForever(a.username, a.password, a.token, a.host, a.port, a.db, a.usessl)
	if err != nil {
		auditLogger.Errorf("create connect error, msg:%s", err)
		return err
	}
	a.conn = conn
	return nil
}

func (a *Audit) createDatabase() error {
	conn, err := db.NewConnectorWithRetryForever(a.username, a.password, a.host, a.port, a.usessl)
	if err != nil {
		auditLogger.Errorf("connect to database error, msg:%s", err)
		return fmt.Errorf("connect to database error, msg:%s", err)
	}
	defer func() { _ = conn.Close() }()
	sql := a.createDBSql()
	auditLogger.Infof("create database, sql:%s", sql)
	_, err = conn.ExecWithRetryForever(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
	if err != nil {
		auditLogger.Errorf("create database error, msg:%s", err)
		return err
	}
	return nil
}

var errNoConnection = errors.New("no connection")

func (a *Audit) createDBSql() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("create database if not exists %s precision 'ns' ", a.db))

	for k, v := range a.dbOptions {
		buf.WriteString(k)
		switch v := v.(type) {
		case string:
			buf.WriteString(fmt.Sprintf(" '%s'", v))
		default:
			buf.WriteString(fmt.Sprintf(" %v", v))
		}
		buf.WriteString(" ")
	}

	return buf.String()
}

func (a *Audit) createSTable() error {
	if a.conn == nil {
		auditLogger.Errorf("create stable operations error, msg:%v", errNoConnection)
		return errNoConnection
	}
	createTableSql := "create stable if not exists operations " +
		"(ts timestamp, user_name varchar(25), operation varchar(20), db varchar(65), " +
		"resource varchar(193), client_address varchar(64), details varchar(50000), " +
		"affected_rows bigint unsigned, `duration` double) " +
		"tags (cluster_id varchar(64))"
	_, err := a.conn.ExecWithRetryForever(context.Background(), createTableSql, util.GetQidOwn(config.Conf.InstanceID))
	if err != nil {
		auditLogger.Errorf("create stable operations error, msg:%s", err)
		return err
	}
	return a.alterSTable()
}

func (a *Audit) alterSTable() error {
	checkSql := "desc operations"
	result, err := a.conn.QueryWithRetryForever(context.Background(), checkSql, util.GetQidOwn(config.Conf.InstanceID))
	if err != nil {
		auditLogger.Errorf("desc stable operations error, msg:%v", err)
		return err
	}

	needAlterClientAddress := false
	needAlterAffectedRows := true
	needAlterDuration := true

	for _, row := range result.Data {
		switch row[0] {
		case "client_address":
			if colLen, ok := row[2].(int32); ok {
				if colLen < 64 {
					needAlterClientAddress = true
				}
			} else {
				auditLogger.Errorf("unexpected type for client_address length: %T", row[2])
				return fmt.Errorf("failed to get client_address column length")
			}
		case "affected_rows":
			needAlterAffectedRows = false
		case "duration":
			needAlterDuration = false
		}
	}

	execAlter := func(sql, msg string) error {
		auditLogger.Info(msg)
		_, err := a.conn.ExecWithRetryForever(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
		if err != nil {
			auditLogger.Errorf("alter stable operations error, sql:%s, msg:%s", sql, err)
			return err
		}
		return nil
	}

	if needAlterClientAddress {
		if err := execAlter("alter stable operations modify column client_address varchar(64)", "alter client_address column to varchar(64)"); err != nil {
			return err
		}
	}
	if needAlterAffectedRows {
		if err := execAlter("alter stable operations add column affected_rows bigint unsigned", "add affected_rows column to stable operations"); err != nil {
			return err
		}
	}
	if needAlterDuration {
		if err := execAlter("alter stable operations add column `duration` double", "add duration column to stable operations"); err != nil {
			return err
		}
	}

	return nil
}
