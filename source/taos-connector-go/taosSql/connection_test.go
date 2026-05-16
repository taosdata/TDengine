package taosSql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
)

// @author: xftan
// @date: 2023/10/13 11:21
// @description: test taos connection exec context
func TestTaosConn_ExecContext(t *testing.T) {
	//nolint:staticcheck
	ctx := context.WithValue(context.Background(), common.ReqIDKey, common.GetReqID())
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	defer func() {
		_, err = execContext(ctx, db, "drop database if exists test_connection")
	}()
	_, err = execContext(ctx, db, "create database if not exists test_connection")
	if err != nil {
		t.Fatal(err)
	}
	_, err = execContext(ctx, db, "use test_connection")
	if err != nil {
		t.Fatal(err)
	}
	_, err = execContext(ctx, db, "create stable if not exists meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = execContext(ctx, db, "INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('?', ?, ?, ?)", "2021-07-13 14:06:32.272", 10.2, 219, 0.32)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := db.QueryContext(ctx, "select count(*) from meters")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = rs.Close()
		assert.NoError(t, err)
	}()
	rs.Next()
	var count int64
	if err = rs.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("result miss")
	}
}

func TestWrongReqID(t *testing.T) {
	//nolint:staticcheck
	ctx := context.WithValue(context.Background(), common.ReqIDKey, uint64(1234))
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	rs, err := db.QueryContext(ctx, "select 1")
	assert.Error(t, err)
	assert.Nil(t, rs)
	_, err = execContext(ctx, db, "create database if not exists test_wrong_req_id")
	assert.Error(t, err)
}
