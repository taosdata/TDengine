package api

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/log"
)

var commonLogger = log.GetLogger("common")

func CreateDatabase(username string, password string, host string, port int, usessl bool, dbname string, databaseOptions map[string]interface{}) {
	ctx := context.Background()

	conn, err := db.NewConnector(username, password, host, port, usessl)
	if err != nil {
		commonLogger.WithError(err).Errorf("connect to adapter error")
		return
	}

	defer closeConn(conn)

	createDBSql := generateCreateDBSql(dbname, databaseOptions)
	commonLogger.Warningf("create database sql: %s", createDBSql)

	for i := 0; i < 3; i++ {
		if _, err := conn.Exec(ctx, createDBSql); err != nil {
			commonLogger.WithError(err).Errorf("try %v times: create database %s error %v", i+1, dbname, err)
			time.Sleep(5 * time.Second)
			continue
		}
		return
	}
	panic(err)
}

func generateCreateDBSql(dbname string, databaseOptions map[string]interface{}) string {
	var buf bytes.Buffer
	buf.WriteString("create database if not exists ")
	buf.WriteString(dbname)

	for k, v := range databaseOptions {
		buf.WriteString(" ")
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

func CreatTables(username string, password string, host string, port int, usessl bool, dbname string, createList []string) {
	ctx := context.Background()
	conn, err := db.NewConnectorWithDb(username, password, host, port, dbname, usessl)
	if err != nil {
		commonLogger.WithError(err).Errorf("connect to database error")
		return
	}
	defer closeConn(conn)

	for _, createSql := range createList {
		commonLogger.Infof("execute sql: %s", createSql)
		if _, err = conn.Exec(ctx, createSql); err != nil {
			commonLogger.Errorf("execute sql: %s, error: %s", createSql, err)
		}
	}
}

func closeConn(conn *db.Connector) {
	if err := conn.Close(); err != nil {
		commonLogger.WithError(err).Errorf("close connection error")
	}
}
