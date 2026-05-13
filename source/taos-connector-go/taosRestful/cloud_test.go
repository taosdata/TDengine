package taosRestful

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCloudRest(t *testing.T) {
	db := "go_test"
	endPoint := os.Getenv("TDENGINE_CLOUD_ENDPOINT")
	token := os.Getenv("TDENGINE_CLOUD_TOKEN")
	if endPoint == "" || token == "" {
		t.Skip("TDENGINE_CLOUD_TOKEN or TDENGINE_CLOUD_ENDPOINT is not set, skip cloud test")
		return
	}
	now := time.Now()
	tbname := fmt.Sprintf("rest_query_test_%d", now.UnixNano())
	t.Log("table name:", tbname)
	dsn := fmt.Sprintf("https(%s:443)/%s?token=%s", endPoint, db, token)
	taos, err := sql.Open("taosRestful", dsn)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		dropTableSql := fmt.Sprintf("drop table if exists %s", tbname)
		res, err := exec(taos, dropTableSql)
		assert.NoError(t, err)
		affected, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), affected)
		err = taos.Close()
		assert.NoError(t, err)
	}()
	createTableSql := fmt.Sprintf("create table if not exists %s (ts timestamp, c1 int, c2 int, c3 int)", tbname)
	res, err := exec(taos, createTableSql)
	assert.NoError(t, err)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), affected)
	insertSql := fmt.Sprintf("insert into %s values (now, 1, 2, 3)", tbname)
	res, err = exec(taos, insertSql)
	assert.NoError(t, err)
	affected, err = res.RowsAffected()
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
}
