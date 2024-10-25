package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/cmd"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/process"
)

var router *gin.Engine
var conf *config.Config
var dbName = "exporter_test"

func TestMain(m *testing.M) {
	conf = config.InitConfig()
	log.ConfigLog()

	conf.Metrics.Database.Name = dbName
	conn, err := db.NewConnector(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.TDengine.Usessl)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ctx := context.Background()
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", conf.Metrics.Database.Name))

	if _, err = conn.Exec(ctx, fmt.Sprintf("create database if not exists %s", dbName)); err != nil {
		logger.Errorf("execute sql: %s, error: %s", fmt.Sprintf("create database %s", dbName), err)
	}
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	reporter := NewReporter(conf)
	reporter.Init(router)

	var createList = []string{
		CreateClusterInfoSql,
		CreateDnodeSql,
		CreateMnodeSql,
		CreateDnodeInfoSql,
		CreateDataDirSql,
		CreateLogDirSql,
		CreateTempDirSql,
		CreateVgroupsInfoSql,
		CreateVnodeRoleSql,
		CreateSummarySql,
		CreateGrantInfoSql,
		CreateKeeperSql,
	}
	CreatTables(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.TDengine.Usessl, conf.Metrics.Database.Name, createList)

	processor := process.NewProcessor(conf)
	node := NewNodeExporter(processor)
	node.Init(router)
	m.Run()
	if _, err = conn.Exec(ctx, fmt.Sprintf("drop database if exists %s", dbName)); err != nil {
		logger.Errorf("execute sql: %s, error: %s", fmt.Sprintf("drop database %s", dbName), err)
	}
}

func TestGetMetrics(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

var now = time.Now()
var nowStr = now.Format(time.RFC3339Nano)

var report = Report{
	Ts:        nowStr,
	DnodeID:   1,
	DnodeEp:   "localhost:7100",
	ClusterID: "6980428120398645172",
	Protocol:  1,
	ClusterInfo: &ClusterInfo{
		FirstEp:          "localhost:7100",
		FirstEpDnodeID:   1,
		Version:          "3.0.0.0",
		MasterUptime:     2.3090276954462752e-05,
		MonitorInterval:  1,
		VgroupsTotal:     2,
		VgroupsAlive:     2,
		VnodesTotal:      2,
		VnodesAlive:      2,
		ConnectionsTotal: 1,
		Dnodes: []Dnode{
			{
				DnodeID: 1,
				DnodeEp: "localhost:7100",
				Status:  "ready",
			},
		},
		Mnodes: []Mnode{
			{
				MnodeID: 1,
				MnodeEp: "localhost:7100",
				Role:    "master",
			},
		},
	},
	VgroupInfos: []VgroupInfo{
		{
			VgroupID:     1,
			DatabaseName: "test",
			TablesNum:    1,
			Status:       "ready",
			Vnodes: []Vnode{
				{
					DnodeID:   1,
					VnodeRole: "LEADER",
				},
				{
					DnodeID:   2,
					VnodeRole: "FOLLOWER",
				},
			},
		},
	},
	GrantInfo: &GrantInfo{
		ExpireTime:      2147483647,
		TimeseriesUsed:  800,
		TimeseriesTotal: 2147483647,
	},
	DnodeInfo: DnodeInfo{
		Uptime:                0.000291412026854232,
		CPUEngine:             0.0828500414250207,
		CPUSystem:             0.4971002485501243,
		CPUCores:              12,
		MemEngine:             9268,
		MemSystem:             54279816,
		MemTotal:              65654816,
		DiskEngine:            0,
		DiskUsed:              39889702912,
		DiskTotal:             210304475136,
		NetIn:                 4727.45292368682,
		NetOut:                2194.251734390486,
		IoRead:                3789.8909811694753,
		IoWrite:               12311.19920713578,
		IoReadDisk:            0,
		IoWriteDisk:           12178.394449950447,
		ReqSelect:             2,
		ReqSelectRate:         0,
		ReqInsert:             6,
		ReqInsertSuccess:      4,
		ReqInsertRate:         0,
		ReqInsertBatch:        10,
		ReqInsertBatchSuccess: 8,
		ReqInsertBatchRate:    0,
		Errors:                2,
		VnodesNum:             2,
		Masters:               2,
		HasMnode:              1,
		HasQnode:              1,
		HasSnode:              1,
		HasBnode:              1,
	},
	DiskInfos: DiskInfo{
		Datadir: []DataDir{
			{
				Name:  "/root/TDengine/sim/dnode1/data",
				Level: 0,
				Avail: decimal.NewFromInt(171049893888),
				Used:  decimal.NewFromInt(39254581248),
				Total: decimal.NewFromInt(210304475136),
			},
			{
				Name:  "/root/TDengine/sim/dnode2/data",
				Level: 1,
				Avail: decimal.NewFromInt(171049893888),
				Used:  decimal.NewFromInt(39254581248),
				Total: decimal.NewFromInt(210304475136),
			},
		},
		Logdir: LogDir{
			Name:  "/root/TDengine/sim/dnode1/log",
			Avail: decimal.NewFromInt(171049771008),
			Used:  decimal.NewFromInt(39254704128),
			Total: decimal.NewFromInt(210304475136),
		},
		Tempdir: TempDir{
			Name:  "/tmp",
			Avail: decimal.NewFromInt(171049771008),
			Used:  decimal.NewFromInt(39254704128),
			Total: decimal.NewFromInt(210304475136),
		},
	},
	LogInfos: LogInfo{
		Summary: []Summary{
			{
				Level: "error",
				Total: 0,
			}, {
				Level: "info",
				Total: 114,
			}, {
				Level: "debug",
				Total: 117,
			}, {
				Level: "trace",
				Total: 126,
			},
		},
	},
}

func TestPutMetrics(t *testing.T) {
	w := httptest.NewRecorder()
	b, _ := json.Marshal(report)
	body := strings.NewReader(string(b))
	req, _ := http.NewRequest(http.MethodPost, "/report", body)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	conn, err := db.NewConnectorWithDb(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host,
		conf.TDengine.Port, dbName, conf.TDengine.Usessl)
	if err != nil {
		logger.WithError(err).Errorf("connect to database error")
		return
	}

	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", conf.Metrics.Database.Name))
	}()

	ctx := context.Background()
	data, err := conn.Query(ctx, "select info from log_summary")
	if err != nil {
		logger.Errorf("execute sql: %s, error: %s", "select * from log_summary", err)
		t.Fatal(err)
	}
	for _, info := range data.Data {
		assert.Equal(t, int32(114), info[0])
	}

	var tenMinutesBefore = now.Add(-10 * time.Minute)
	var tenMinutesBeforeStr = tenMinutesBefore.Format(time.RFC3339Nano)

	conf.FromTime = tenMinutesBeforeStr
	conf.Transfer = "old_taosd_metric"

	var cmd = cmd.NewCommand(conf)
	cmd.Process(conf)

	type TableInfo struct {
		TsName string
		RowNum int
	}

	tables := map[string]*TableInfo{
		"taosd_cluster_basic":    {"ts", 1},
		"taosd_cluster_info":     {"_ts", 1},
		"taosd_vgroups_info":     {"_ts", 1},
		"taosd_dnodes_info":      {"_ts", 1},
		"taosd_dnodes_status":    {"_ts", 1},
		"taosd_dnodes_data_dirs": {"_ts", 1},
		"taosd_dnodes_log_dirs":  {"_ts", 2},
		"taosd_mnodes_info":      {"_ts", 1},
		"taosd_vnodes_info":      {"_ts", 1},
	}

	for table, tableInfo := range tables {
		data, err = conn.Query(ctx, fmt.Sprintf("select %s from %s", tableInfo.TsName, table))
		if err != nil {
			logger.Errorf("execute sql: %s, error: %s", "select * from "+table, err)
			t.Fatal(err)
		}

		assert.Equal(t, tableInfo.RowNum, len(data.Data))
		assert.Equal(t, now.UnixMilli(), data.Data[0][0].(time.Time).UnixMilli())
	}

	conf.Transfer = ""
	conf.Drop = "old_taosd_metric_stables"
	cmd.Process(conf)

	data, err = conn.Query(ctx, "select * from  information_schema.ins_stables where stable_name = 'm_info'")
	if err != nil {
		logger.Errorf("execute sql: %s, error: %s", "m_info is not droped", err)
		t.Fatal(err)
	}
	assert.Equal(t, 0, len(data.Data))
	logger.Infof("ALL  OK  !!!")
}
