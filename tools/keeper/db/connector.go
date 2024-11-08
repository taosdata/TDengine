package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"

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

func NewConnector(username, password, host string, port int, usessl bool) (*Connector, error) {
	var protocol string
	if usessl {
		protocol = "https"
	} else {
		protocol = "http"
	}
	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn()})
	dbLogger.Tracef("connect to adapter, host:%s, port:%d, usessl:%v", host, port, usessl)

	db, err := sql.Open("taosRestful", fmt.Sprintf("%s:%s@%s(%s:%d)/?skipVerify=true", username, password, protocol, host, port))
	if err != nil {
		dbLogger.Errorf("connect to adapter failed, host:%s, port:%d, usessl:%v, error:%s", host, port, usessl, err)
		return nil, err
	}

	dbLogger.Tracef("connect to adapter success, host:%s, port:%d, usessl:%v", host, port, usessl)
	return &Connector{db: db}, nil
}

func NewConnectorWithDb(username, password, host string, port int, dbname string, usessl bool) (*Connector, error) {
	var protocol string
	if usessl {
		protocol = "https"
	} else {
		protocol = "http"
	}

	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: util.GetQidOwn()})
	dbLogger.Tracef("connect to adapter, host:%s, port:%d, usessl:%v", host, port, usessl)

	db, err := sql.Open("taosRestful", fmt.Sprintf("%s:%s@%s(%s:%d)/%s?skipVerify=true", username, password, protocol, host, port, dbname))
	if err != nil {
		dbLogger.Errorf("connect to adapter failed, host:%s, port:%d, db:%s, usessl:%v, error:%s", host, port, dbname, usessl, err)
		return nil, err
	}

	dbLogger.Tracef("connect to adapter success, host:%s, port:%d, db:%s, usessl:%v", host, port, dbname, usessl)
	return &Connector{db: db}, nil
}

func (c *Connector) Exec(ctx context.Context, sql string, qid uint64) (int64, error) {
	dbLogger := dbLogger.WithFields(logrus.Fields{config.ReqIDKey: qid})
	ctx = context.WithValue(ctx, common.ReqIDKey, int64(qid))

	dbLogger.Tracef("call adapter to execute sql:%s", sql)
	startTime := time.Now()
	res, err := c.db.ExecContext(ctx, sql)

	endTime := time.Now()
	latency := endTime.Sub(startTime)

	if err != nil {
		if strings.Contains(err.Error(), "Authentication failure") {
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
	ctx = context.WithValue(ctx, common.ReqIDKey, int64(qid))

	dbLogger.Tracef("call adapter to execute query, sql:%s", sql)

	startTime := time.Now()
	rows, err := c.db.QueryContext(ctx, sql)

	endTime := time.Now()
	latency := endTime.Sub(startTime)

	if err != nil {
		if strings.Contains(err.Error(), "Authentication failure") {
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
