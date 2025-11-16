package system

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kardianos/service"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func TestInit(t *testing.T) {
	if os.Getenv("TAOS_KEEPER_RUN_INIT_TEST") != "1" {
		t.Skip("skipping Init integration test; set TAOS_KEEPER_RUN_INIT_TEST=1 to enable")
	}
	addr := fmt.Sprintf("%s:%d", config.Conf.TDengine.Host, config.Conf.TDengine.Port)
	if _, err := net.DialTimeout("tcp", addr, 2*time.Second); err != nil {
		t.Skip("taosAdapter REST not available; skipping integration test")
	}
	server := Init()
	assert.NotNil(t, server)

	conn, err := db.NewConnectorWithDb(config.Conf.TDengine.Username, config.Conf.TDengine.Password, config.Conf.TDengine.Host, config.Conf.TDengine.Port, config.Conf.Metrics.Database.Name, config.Conf.TDengine.Usessl)
	assert.NoError(t, err)
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", config.Conf.Metrics.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", config.Conf.Audit.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
}

func Test_program(t *testing.T) {
	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	prg := newProgram(server)
	svcConfig := &service.Config{
		Name:        "taoskeeper",
		DisplayName: "taoskeeper",
		Description: "taosKeeper is a tool for TDengine that exports monitoring metrics",
	}
	svc, err := service.New(prg, svcConfig)
	assert.NoError(t, err)

	err = prg.Start(svc)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = prg.Stop(svc)
	assert.NoError(t, err)
}
