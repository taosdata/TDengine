package taosWS

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCloudWS(t *testing.T) {
	db := "go_test"
	endPoint := os.Getenv("TDENGINE_CLOUD_ENDPOINT")
	token := os.Getenv("TDENGINE_CLOUD_TOKEN")
	if endPoint == "" || token == "" {
		t.Skip("TDENGINE_CLOUD_TOKEN or TDENGINE_CLOUD_ENDPOINT is not set, skip cloud test")
		return
	}
	now := time.Now()
	tbname := fmt.Sprintf("ws_query_test_%d", now.UnixNano())
	t.Log("table name:", tbname)
	stmtTbName := fmt.Sprintf("ws_stmt_test_%d", now.UnixNano())
	t.Log("stmt tbname:", stmtTbName)
	dsn := fmt.Sprintf("wss(%s:443)/%s?token=%s", endPoint, db, token)
	taos, err := sql.Open("taosWS", dsn)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		dropSqls := []string{
			fmt.Sprintf("drop table if exists %s", tbname),
			fmt.Sprintf("drop table if exists %s", stmtTbName),
		}
		for i := 0; i < len(dropSqls); i++ {
			dropTableSql := dropSqls[i]
			res, err := exec(taos, dropTableSql)
			assert.NoError(t, err)
			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(0), affected)
		}

		err = taos.Close()
		assert.NoError(t, err)
	}()
	createSqls := []string{
		fmt.Sprintf("create table if not exists %s (ts timestamp, c1 int, c2 int, c3 int)", tbname),
		fmt.Sprintf("create table if not exists %s (ts timestamp, c1 int, c2 int, c3 int)", stmtTbName),
	}
	for i := 0; i < len(createSqls); i++ {
		createTableSql := createSqls[i]
		res, err := exec(taos, createTableSql)
		assert.NoError(t, err)
		affected, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), affected)
	}
	insertSql := fmt.Sprintf("insert into %s values (now, 1, 2, 3)", tbname)
	res, err := exec(taos, insertSql)
	assert.NoError(t, err)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	querySql := fmt.Sprintf("select * from %s", tbname)
	rows, err := taos.Query(querySql)
	assert.NoError(t, err)
	defer func() {
		err = rows.Close()
		assert.NoError(t, err)
	}()
	var ts time.Time
	var c1, c2, c3 int
	var rowCount int
	for rows.Next() {
		rowCount++
		err = rows.Scan(&ts, &c1, &c2, &c3)
		assert.NoError(t, err)
		assert.Equal(t, 1, c1)
		assert.Equal(t, 2, c2)
		assert.Equal(t, 3, c3)
		t.Logf("ts: %s, c1: %d, c2: %d, c3: %d", ts.Format(time.RFC3339), c1, c2, c3)
	}
	assert.Equal(t, 1, rowCount)
	stmt, err := taos.Prepare(fmt.Sprintf("insert into %s values (?, ?, ?, ?)", stmtTbName))
	assert.NoError(t, err)
	defer func() {
		err = stmt.Close()
		assert.NoError(t, err)
	}()
	res, err = stmt.Exec(now, 4, 5, 6)
	assert.NoError(t, err)
	affected, err = res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	querySql = fmt.Sprintf("select * from %s where ts = ?", stmtTbName)
	queryStmt, err := taos.Prepare(querySql)
	assert.NoError(t, err)
	defer func() {
		err = stmt.Close()
		assert.NoError(t, err)
	}()
	rows, err = queryStmt.Query(now)
	assert.NoError(t, err)
	defer func() {
		err = rows.Close()
		assert.NoError(t, err)
	}()
	rowCount = 0
	for rows.Next() {
		rowCount++
		err = rows.Scan(&ts, &c1, &c2, &c3)
		assert.NoError(t, err)
		assert.Equal(t, 4, c1)
		assert.Equal(t, 5, c2)
		assert.Equal(t, 6, c3)
		t.Logf("ts: %s, c1: %d, c2: %d, c3: %d", ts.Format(time.RFC3339), c1, c2, c3)
	}
	assert.Equal(t, 1, rowCount)
}
