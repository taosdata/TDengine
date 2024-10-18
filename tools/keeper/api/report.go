package api

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/go-utils/json"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

var logger = log.GetLogger("REP")

var createList = []string{
	// CreateClusterInfoSql,
	// CreateDnodeSql,
	// CreateMnodeSql,
	// CreateDnodeInfoSql,
	// CreateDataDirSql,
	// CreateLogDirSql,
	// CreateTempDirSql,
	// CreateVgroupsInfoSql,
	// CreateVnodeRoleSql,
	// CreateSummarySql,
	// CreateGrantInfoSql,
	CreateKeeperSql,
}

type Reporter struct {
	username        string
	password        string
	host            string
	port            int
	usessl          bool
	dbname          string
	databaseOptions map[string]interface{}
	totalRep        atomic.Value
}

func NewReporter(conf *config.Config) *Reporter {
	r := &Reporter{
		username:        conf.TDengine.Username,
		password:        conf.TDengine.Password,
		host:            conf.TDengine.Host,
		port:            conf.TDengine.Port,
		usessl:          conf.TDengine.Usessl,
		dbname:          conf.Metrics.Database.Name,
		databaseOptions: conf.Metrics.Database.Options,
	}
	r.totalRep.Store(0)
	return r
}

func (r *Reporter) Init(c gin.IRouter) {
	c.POST("report", r.handlerFunc())
	r.createDatabase()
	r.creatTables()
	// todo: it can delete in the future.
	if r.shouldDetectFields() {
		r.detectGrantInfoFieldType()
		r.detectClusterInfoFieldType()
		r.detectVgroupsInfoType()
	}
}

func (r *Reporter) getConn() *db.Connector {
	conn, err := db.NewConnector(r.username, r.password, r.host, r.port, r.usessl)
	if err != nil {
		qid := util.GetQidOwn()

		logger := logger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)
		logger.Errorf("connect to database error, msg:%s", err)
		panic(err)
	}
	return conn
}

func (r *Reporter) detectGrantInfoFieldType() {
	// `expire_time` `timeseries_used` `timeseries_total` in table `grant_info` changed to bigint from TS-3003.
	ctx := context.Background()
	conn := r.getConn()
	defer r.closeConn(conn)

	r.detectFieldType(ctx, conn, "grants_info", "expire_time", "bigint")
	r.detectFieldType(ctx, conn, "grants_info", "timeseries_used", "bigint")
	r.detectFieldType(ctx, conn, "grants_info", "timeseries_total", "bigint")
	if r.tagExist(ctx, conn, "grants_info", "dnode_id") {
		r.dropTag(ctx, conn, "grants_info", "dnode_id")
	}
	if r.tagExist(ctx, conn, "grants_info", "dnode_ep") {
		r.dropTag(ctx, conn, "grants_info", "dnode_ep")
	}
}

func (r *Reporter) detectClusterInfoFieldType() {
	// `tbs_total` in table `cluster_info` changed to bigint from TS-3003.
	ctx := context.Background()
	conn := r.getConn()
	defer r.closeConn(conn)

	r.detectFieldType(ctx, conn, "cluster_info", "tbs_total", "bigint")

	// add column `topics_total` and `streams_total` from TD-22032
	// if exists, _ := r.columnInfo(ctx, conn, "cluster_info", "topics_total"); !exists {
	// 	logger.Warningf("## %s.cluster_info.topics_total not exists, will add it", r.dbname)
	// 	r.addColumn(ctx, conn, "cluster_info", "topics_total", "int")
	// }
	// if exists, _ := r.columnInfo(ctx, conn, "cluster_info", "streams_total"); !exists {
	// 	logger.Warningf("## %s.cluster_info.streams_total not exists, will add it", r.dbname)
	// 	r.addColumn(ctx, conn, "cluster_info", "streams_total", "int")
	// }
}

func (r *Reporter) detectVgroupsInfoType() {
	// `tables_num` in table `vgroups_info` changed to bigint from TS-3003.
	ctx := context.Background()
	conn := r.getConn()
	defer r.closeConn(conn)

	r.detectFieldType(ctx, conn, "vgroups_info", "tables_num", "bigint")
}

func (r *Reporter) detectFieldType(ctx context.Context, conn *db.Connector, table, field, fieldType string) {
	_, colType := r.columnInfo(ctx, conn, table, field)
	if colType == "INT" {
		logger.Warningf("%s.%s.%s type is %s, will change to %s", r.dbname, table, field, colType, fieldType)
		// drop column `tables_num`
		r.dropColumn(ctx, conn, table, field)

		// add column `tables_num`
		r.addColumn(ctx, conn, table, field, fieldType)
	}
}

func (r *Reporter) shouldDetectFields() bool {
	ctx := context.Background()
	conn := r.getConn()
	defer r.closeConn(conn)

	version, err := r.serverVersion(ctx, conn)
	if err != nil {
		logger.Errorf("get server version error:%s", err)
		return false
	}

	// if server version is less than v3.0.3.0, should not detect fields.
	versions := strings.Split(version, ".")
	if len(versions) < 4 {
		logger.Errorf("get server version error. version:%s", version)
		return false
	}

	v1, _ := strconv.Atoi(versions[0])
	v2, _ := strconv.Atoi(versions[1])
	v3, _ := strconv.Atoi(versions[2])

	if v1 > 3 || v2 > 0 || v3 >= 3 {
		return true
	}

	return false
}

func (r *Reporter) serverVersion(ctx context.Context, conn *db.Connector) (version string, err error) {
	res, err := conn.Query(ctx, "select server_version()", util.GetQidOwn())
	if err != nil {
		logger.Errorf("get server version error, msg:%s", err)
		return
	}

	if len(res.Data) == 0 {
		logger.Errorf("get server version error. response:%+v", res)
		return
	}

	if len(res.Data) != 1 && len(res.Data[0]) != 1 {
		logger.Errorf("get server version error. response:%+v", res)
		return
	}

	version = res.Data[0][0].(string)

	return
}

func (r *Reporter) columnInfo(ctx context.Context, conn *db.Connector, table string, field string) (exists bool, colType string) {
	res, err := conn.Query(ctx, fmt.Sprintf("select col_type from information_schema.ins_columns where table_name='%s' and db_name='%s' and col_name='%s'", table, r.dbname, field), util.GetQidOwn())
	if err != nil {
		logger.Errorf("get %s field type error, msg:%s", r.dbname, err)
		panic(err)
	}

	if len(res.Data) == 0 {
		return
	}

	if len(res.Data) != 1 && len(res.Data[0]) != 1 {
		logger.Errorf("get field type for %s error. response:%+v", table, res)
		panic(fmt.Sprintf("get field type for %s error. response:%+v", table, res))
	}

	exists = true
	colType = res.Data[0][0].(string)
	colType = strings.ToUpper(colType)
	return
}

func (r *Reporter) tagExist(ctx context.Context, conn *db.Connector, stable string, tag string) (exists bool) {
	res, err := conn.Query(ctx, fmt.Sprintf("select tag_name from information_schema.ins_tags where stable_name='%s' and db_name='%s' and tag_name='%s'", stable, r.dbname, tag), util.GetQidOwn())
	if err != nil {
		logger.Errorf("get %s tag_name error, msg:%s", r.dbname, err)
		panic(err)
	}

	if len(res.Data) == 0 {
		exists = false
		return
	}

	if len(res.Data) != 1 && len(res.Data[0]) != 1 {
		logger.Errorf("get tag_name for %s error. response:%+v", stable, res)
		panic(fmt.Sprintf("get tag_name for %s error. response:%+v", stable, res))
	}

	exists = true
	return
}

func (r *Reporter) dropColumn(ctx context.Context, conn *db.Connector, table string, field string) {
	if _, err := conn.Exec(ctx, fmt.Sprintf("alter table %s.%s drop column %s", r.dbname, table, field), util.GetQidOwn()); err != nil {
		logger.Errorf("drop column %s from table %s error, msg:%s", field, table, err)
		panic(err)
	}
}

func (r *Reporter) dropTag(ctx context.Context, conn *db.Connector, stable string, tag string) {
	if _, err := conn.Exec(ctx, fmt.Sprintf("alter stable %s.%s drop tag %s", r.dbname, stable, tag), util.GetQidOwn()); err != nil {
		logger.Errorf("drop tag %s from stable %s error, msg:%s", tag, stable, err)
		panic(err)
	}
}

func (r *Reporter) addColumn(ctx context.Context, conn *db.Connector, table string, field string, fieldType string) {
	if _, err := conn.Exec(ctx, fmt.Sprintf("alter table %s.%s add column %s %s", r.dbname, table, field, fieldType), util.GetQidOwn()); err != nil {
		logger.Errorf("add column %s to table %s error, msg:%s", field, table, err)
		panic(err)
	}
}

func (r *Reporter) createDatabase() {
	ctx := context.Background()
	conn := r.getConn()
	defer r.closeConn(conn)

	createDBSql := r.generateCreateDBSql()
	logger.Warningf("create database sql: %s", createDBSql)

	if _, err := conn.Exec(ctx, createDBSql, util.GetQidOwn()); err != nil {
		logger.Errorf("create database %s error, msg:%v", r.dbname, err)
		panic(err)
	}
}

func (r *Reporter) generateCreateDBSql() string {
	var buf bytes.Buffer
	buf.WriteString("create database if not exists ")
	buf.WriteString(r.dbname)

	for k, v := range r.databaseOptions {
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

func (r *Reporter) creatTables() {
	ctx := context.Background()
	conn, err := db.NewConnectorWithDb(r.username, r.password, r.host, r.port, r.dbname, r.usessl)
	if err != nil {
		logger.Errorf("connect to database error, msg:%s", err)
		return
	}
	defer r.closeConn(conn)

	for _, createSql := range createList {
		logger.Infof("execute sql:%s", createSql)
		if _, err = conn.Exec(ctx, createSql, util.GetQidOwn()); err != nil {
			logger.Errorf("execute sql:%s, error:%s", createSql, err)
		}
	}
}

func (r *Reporter) closeConn(conn *db.Connector) {
	if err := conn.Close(); err != nil {
		logger.Errorf("close connection error, msg:%s", err)
	}
}

func (r *Reporter) handlerFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		logger := logger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		r.recordTotalRep()
		// data parse
		data, err := c.GetRawData()
		if err != nil {
			logger.Errorf("receiving taosd data error, msg:%s", err)
			return
		}
		var report Report

		logger.Tracef("report data:%s", string(data))
		if e := json.Unmarshal(data, &report); e != nil {
			logger.Errorf("error occurred while unmarshal request, data:%s, error:%s", data, err)
			return
		}
		var sqls []string
		if report.ClusterInfo != nil {
			sqls = append(sqls, insertClusterInfoSql(*report.ClusterInfo, report.ClusterID, report.Protocol, report.Ts)...)
		}
		sqls = append(sqls, insertDnodeSql(report.DnodeInfo, report.DnodeID, report.DnodeEp, report.ClusterID, report.Ts))
		if report.GrantInfo != nil {
			sqls = append(sqls, insertGrantSql(*report.GrantInfo, report.DnodeID, report.ClusterID, report.Ts))
		}
		sqls = append(sqls, insertDataDirSql(report.DiskInfos, report.DnodeID, report.DnodeEp, report.ClusterID, report.Ts)...)
		for _, group := range report.VgroupInfos {
			sqls = append(sqls, insertVgroupSql(group, report.DnodeID, report.DnodeEp, report.ClusterID, report.Ts)...)
		}
		sqls = append(sqls, insertLogSummary(report.LogInfos, report.DnodeID, report.DnodeEp, report.ClusterID, report.Ts))

		conn, err := db.NewConnectorWithDb(r.username, r.password, r.host, r.port, r.dbname, r.usessl)
		if err != nil {
			logger.Errorf("connect to database error, msg:%s", err)
			return
		}
		defer r.closeConn(conn)
		ctx := context.Background()

		for _, sql := range sqls {
			logger.Tracef("execute sql:%s", sql)
			if _, err := conn.Exec(ctx, sql, util.GetQidOwn()); err != nil {
				logger.Errorf("execute sql error, sql:%s, error:%s", sql, err)
			}
		}
	}
}

func (r *Reporter) recordTotalRep() {
	old := r.totalRep.Load().(int)
	for i := 0; i < 3; i++ {
		r.totalRep.CompareAndSwap(old, old+1)
	}
}

func (r *Reporter) GetTotalRep() *atomic.Value {
	return &r.totalRep
}

func insertClusterInfoSql(info ClusterInfo, ClusterID string, protocol int, ts string) []string {
	var sqls []string
	var dtotal, dalive, mtotal, malive int
	for _, dnode := range info.Dnodes {
		sqls = append(sqls, fmt.Sprintf("insert into d_info_%s using d_info tags (%d, '%s', '%s') values ('%s', '%s')",
			ClusterID+strconv.Itoa(dnode.DnodeID), dnode.DnodeID, dnode.DnodeEp, ClusterID, ts, dnode.Status))
		dtotal++
		if "ready" == dnode.Status {
			dalive++
		}
	}

	for _, mnode := range info.Mnodes {
		sqls = append(sqls, fmt.Sprintf("insert into m_info_%s using m_info tags (%d, '%s', '%s') values ('%s', '%s')",
			ClusterID+strconv.Itoa(mnode.MnodeID), mnode.MnodeID, mnode.MnodeEp, ClusterID, ts, mnode.Role))
		mtotal++
		//LEADER FOLLOWER CANDIDATE ERROR
		if "ERROR" != mnode.Role {
			malive++
		}
	}

	sqls = append(sqls, fmt.Sprintf(
		"insert into cluster_info_%s using cluster_info tags('%s') (ts, first_ep, first_ep_dnode_id, version, "+
			"master_uptime, monitor_interval, dbs_total, tbs_total, stbs_total, dnodes_total, dnodes_alive, "+
			"mnodes_total, mnodes_alive, vgroups_total, vgroups_alive, vnodes_total, vnodes_alive, connections_total, "+
			"topics_total, streams_total, protocol) values ('%s', '%s', %d, '%s', %f, %d, %d, %d, %d, %d, %d, %d, %d, "+
			"%d, %d, %d, %d, %d, %d, %d, %d)",
		ClusterID, ClusterID, ts, info.FirstEp, info.FirstEpDnodeID, info.Version, info.MasterUptime, info.MonitorInterval,
		info.DbsTotal, info.TbsTotal, info.StbsTotal, dtotal, dalive, mtotal, malive, info.VgroupsTotal, info.VgroupsAlive,
		info.VnodesTotal, info.VnodesAlive, info.ConnectionsTotal, info.TopicsTotal, info.StreamsTotal, protocol))
	return sqls
}

func insertDnodeSql(info DnodeInfo, DnodeID int, DnodeEp string, ClusterID string, ts string) string {
	return fmt.Sprintf("insert into dnode_info_%s using dnodes_info tags (%d, '%s', '%s') values ('%s', %f, %f, %f, %f, %d, %d, %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %d, %f, %d, %d, %f, %d, %d, %f, %d, %d, %d, %d, %d, %d, %d)",
		ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID,
		ts, info.Uptime, info.CPUEngine, info.CPUSystem, info.CPUCores, info.MemEngine, info.MemSystem, info.MemTotal,
		info.DiskEngine, info.DiskUsed, info.DiskTotal, info.NetIn, info.NetOut, info.IoRead, info.IoWrite,
		info.IoReadDisk, info.IoWriteDisk, info.ReqSelect, info.ReqSelectRate, info.ReqInsert, info.ReqInsertSuccess,
		info.ReqInsertRate, info.ReqInsertBatch, info.ReqInsertBatchSuccess, info.ReqInsertBatchRate, info.Errors,
		info.VnodesNum, info.Masters, info.HasMnode, info.HasQnode, info.HasSnode, info.HasBnode)
}

func insertDataDirSql(disk DiskInfo, DnodeID int, DnodeEp string, ClusterID string, ts string) []string {
	var sqls []string
	for _, data := range disk.Datadir {
		sqls = append(sqls,
			fmt.Sprintf("insert into data_dir_%s using data_dir tags (%d, '%s', '%s') values ('%s', '%s', %d, %d, %d, %d)",
				ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID,
				ts, data.Name, data.Level, data.Avail.IntPart(), data.Used.IntPart(), data.Total.IntPart()),
		)
	}
	sqls = append(sqls,
		fmt.Sprintf("insert into log_dir_%s using log_dir tags (%d, '%s', '%s') values ('%s', '%s', %d, %d, %d)",
			ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID,
			ts, disk.Logdir.Name, disk.Logdir.Avail.IntPart(), disk.Logdir.Used.IntPart(), disk.Logdir.Total.IntPart()),
		fmt.Sprintf("insert into temp_dir_%s using temp_dir tags (%d, '%s', '%s') values ('%s', '%s', %d, %d, %d)",
			ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID,
			ts, disk.Tempdir.Name, disk.Tempdir.Avail.IntPart(), disk.Tempdir.Used.IntPart(), disk.Tempdir.Total.IntPart()),
	)
	return sqls
}

func insertVgroupSql(g VgroupInfo, DnodeID int, DnodeEp string, ClusterID string, ts string) []string {
	var sqls []string
	sqls = append(sqls, fmt.Sprintf("insert into vgroups_info_%s using vgroups_info tags (%d, '%s', '%s') "+
		"(ts, vgroup_id, database_name, tables_num, status, ) values ( '%s','%d', '%s', %d, '%s')",
		ClusterID+strconv.Itoa(DnodeID)+strconv.Itoa(g.VgroupID), DnodeID, DnodeEp, ClusterID,
		ts, g.VgroupID, g.DatabaseName, g.TablesNum, g.Status))
	for _, v := range g.Vnodes {
		sqls = append(sqls, fmt.Sprintf("insert into vnodes_role_%s using vnodes_role tags (%d, '%s', '%s') values ('%s', '%s')",
			ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID, ts, v.VnodeRole))
	}
	return sqls
}

func insertLogSummary(log LogInfo, DnodeID int, DnodeEp string, ClusterID string, ts string) string {
	var e, info, debug, trace int
	for _, s := range log.Summary {
		switch s.Level {
		case "error":
			e = s.Total
		case "info":
			info = s.Total
		case "debug":
			debug = s.Total
		case "trace":
			trace = s.Total
		}
	}
	return fmt.Sprintf("insert into log_summary_%s using log_summary tags (%d, '%s', '%s') values ('%s', %d, %d, %d, %d)",
		ClusterID+strconv.Itoa(DnodeID), DnodeID, DnodeEp, ClusterID, ts, e, info, debug, trace)
}

func insertGrantSql(g GrantInfo, DnodeID int, ClusterID string, ts string) string {
	return fmt.Sprintf("insert into grants_info_%s using grants_info tags ('%s') (ts, expire_time, "+
		"timeseries_used, timeseries_total) values ('%s', %d, %d, %d)", ClusterID+strconv.Itoa(DnodeID), ClusterID, ts, g.ExpireTime, g.TimeseriesUsed, g.TimeseriesTotal)
}
