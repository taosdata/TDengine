package system

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestStart(t *testing.T) {
	server := Init()
	assert.NotNil(t, server)

	conn, err := db.NewConnectorWithDb(config.Conf.TDengine.Username, config.Conf.TDengine.Password, config.Conf.TDengine.Host, config.Conf.TDengine.Port, config.Conf.Metrics.Database.Name, config.Conf.TDengine.Usessl)
	assert.NoError(t, err)
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", config.Conf.Metrics.Database.Name))
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", config.Conf.Audit.Database.Name))
}
