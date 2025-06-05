package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestIPv6(t *testing.T) {
	config.InitConfig()

	// [::1]
	conn, err := NewConnector("root", "taosdata", "127.0.0.1", 6041, false)
	assert.NoError(t, err)

	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_ipv6", 1001)
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "create database test_ipv6", 1002)
	assert.NoError(t, err)

	// [::1]
	conn, err = NewConnectorWithDb("root", "taosdata", "127.0.0.1", 6041, "test_ipv6", false)
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
