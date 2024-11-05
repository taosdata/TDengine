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
}

type AuditInfo struct {
	Timestamp string `json:"timestamp"`
	ClusterID string `json:"cluster_id"`
	User      string `json:"user"`
	Operation string `json:"operation"`
	Db        string `json:"db"`
	Resource  string `json:"resource"`
	ClientAdd string `json:"client_add"` // client address
	Details   string `json:"details"`
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

func NewAudit(c *config.Config) (*Audit, error) {
	a := Audit{
		username:  c.TDengine.Username,
		password:  c.TDengine.Password,
		host:      c.TDengine.Host,
		port:      c.TDengine.Port,
		usessl:    c.TDengine.Usessl,
		db:        c.Audit.Database.Name,
		dbOptions: c.Audit.Database.Options,
	}
	if a.db == "" {
		a.db = "audit"
	}
	return &a, nil
}

func (a *Audit) Init(c gin.IRouter) error {
	if err := a.createDatabase(); err != nil {
		return fmt.Errorf("create database error, msg:%s", err)
	}
	if err := a.initConnect(); err != nil {
		return fmt.Errorf("init db connect error, msg:%s", err)
	}
	if err := a.createSTables(); err != nil {
		return fmt.Errorf("create stable error, msg:%s", err)
	}
	c.POST("/audit", a.handleFunc())
	c.POST("/audit_v2", a.handleFunc())
	c.POST("/audit-batch", a.handleBatchFunc())
	return nil
}

func (a *Audit) handleBatchFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		auditLogger := auditLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

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

		err = handleBatchRecord(auditArray.Records, a.conn, qid)

		if err != nil {
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
	details := handleDetails(audit.Details)

	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values (%s, '%s', '%s', '%s', '%s', '%s', '%s') ",
		getTableName(audit), audit.ClusterID, audit.Timestamp, audit.User, audit.Operation, audit.Db, audit.Resource, audit.ClientAdd, details)
}

func parseSqlOld(audit AuditInfoOld) string {
	details := handleDetails(audit.Details)

	return fmt.Sprintf(
		"insert into %s using operations tags ('%s') values (%s, '%s', '%s', '%s', '%s', '%s', '%s') ",
		getTableNameOld(audit), audit.ClusterID, strconv.FormatInt(audit.Timestamp, 10)+"000000", audit.User, audit.Operation, audit.Db, audit.Resource, audit.ClientAdd, details)
}

func handleBatchRecord(auditArray []AuditInfo, conn *db.Connector, qid uint64) error {
	var builder strings.Builder
	var head = fmt.Sprintf(
		"insert into %s using operations tags ('%s') values",
		getTableName(auditArray[0]), auditArray[0].ClusterID)

	builder.WriteString(head)
	var qid_counter uint8 = 0
	for _, audit := range auditArray {

		details := handleDetails(audit.Details)
		valuesStr := fmt.Sprintf(
			"(%s, '%s', '%s', '%s', '%s', '%s', '%s') ",
			audit.Timestamp, audit.User, audit.Operation, audit.Db, audit.Resource, audit.ClientAdd, details)

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

func (a *Audit) initConnect() error {
	conn, err := db.NewConnectorWithDb(a.username, a.password, a.host, a.port, a.db, a.usessl)
	if err != nil {
		auditLogger.Errorf("init db connect error, msg:%s", err)
		return err
	}
	a.conn = conn
	return nil
}

func (a *Audit) createDatabase() error {
	conn, err := db.NewConnector(a.username, a.password, a.host, a.port, a.usessl)
	if err != nil {
		return fmt.Errorf("connect to database error, msg:%s", err)
	}
	defer func() { _ = conn.Close() }()
	sql := a.createDBSql()
	auditLogger.Infof("create database, sql:%s", sql)
	_, err = conn.Exec(context.Background(), sql, util.GetQidOwn())
	if err != nil {
		auditLogger.Errorf("create database error, msg:%s", err)
		return err
	}
	return err
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

func (a *Audit) createSTables() error {
	var createTableSql = "create stable if not exists operations " +
		"(ts timestamp, user_name varchar(25), operation varchar(20), db varchar(65), resource varchar(193), client_address varchar(25), details varchar(50000)) " +
		"tags (cluster_id varchar(64))"

	if a.conn == nil {
		return errNoConnection
	}
	_, err := a.conn.Exec(context.Background(), createTableSql, util.GetQidOwn())
	if err != nil {
		auditLogger.Errorf("## create stable error, msg:%s", err)
		return err
	}
	return nil
}
