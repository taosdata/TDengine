package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

			sql = parseSql(audit)
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

func (a *Audit) prepareConnectionAndTable(c *gin.Context, logger *logrus.Entry) bool {
	db := c.Query("db")
	token := c.Query("token")

	isDefault := db == ""
	if isDefault {
		db = getAuditDBName(config.Conf)
	}

	logger.Tracef("prepare audit connection for db: %s, token is empty: %v", db, token == "")

	if !a.inited || db != a.db || token != a.token {
		a.mu.Lock()
		defer a.mu.Unlock()
		if !a.inited || db != a.db || token != a.token {
			logger.Infof("create audit connection for db: %s", db)
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

func handleDetails(details string) string {
	if strings.Contains(details, "'") {
		details = strings.ReplaceAll(details, "'", "\\'")
	}
	if strings.Contains(details, "\"") {
		details = strings.ReplaceAll(details, "\"", "\\\"")
	}
	if len(details) > MAX_DETAIL_LEN {
		details = details[:MAX_DETAIL_LEN]
	}
	return details
}

func parseSql(audit AuditInfo) string {
	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values (%s, '%s', '%s', '%s', '%s', '%s', '%s', %d, %f)",
		getTableName(audit), audit.ClusterID, audit.Timestamp, audit.User, audit.Operation, audit.Db, audit.Resource,
		audit.ClientAdd, handleDetails(audit.Details), audit.AffectedRows, audit.Duration)
}

func parseSqlOld(audit AuditInfoOld) string {
	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values (%s, '%s', '%s', '%s', '%s', '%s', '%s', 0, 0)",
		getTableNameOld(audit), audit.ClusterID, strconv.FormatInt(audit.Timestamp, 10)+"000000", audit.User,
		audit.Operation, audit.Db, audit.Resource, audit.ClientAdd, handleDetails(audit.Details))
}

func handleBatchRecord(auditArray []AuditInfo, conn *db.Connector, qid uint64) error {
	var builder strings.Builder
	var head = fmt.Sprintf(
		"insert into %s using operations tags ('%s') values",
		getTableName(auditArray[0]), auditArray[0].ClusterID)

	builder.WriteString(head)
	var qid_counter uint8 = 0
	for _, audit := range auditArray {
		valuesStr := fmt.Sprintf(
			"(%s, '%s', '%s', '%s', '%s', '%s', '%s', %d, %f) ",
			audit.Timestamp, audit.User, audit.Operation, audit.Db, audit.Resource,
			audit.ClientAdd, handleDetails(audit.Details), audit.AffectedRows, audit.Duration)

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
	return fmt.Sprintf("t_operations_%s", audit.ClusterID)
}

func getTableNameOld(audit AuditInfoOld) string {
	return fmt.Sprintf("t_operations_%s", audit.ClusterID)
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
