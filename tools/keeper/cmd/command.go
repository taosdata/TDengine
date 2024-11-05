package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
	"github.com/taosdata/taoskeeper/util/pool"
)

var logger = log.GetLogger("CMD")

var MAX_SQL_LEN = 1000000

type Command struct {
	fromTime time.Time
	client   *http.Client
	conn     *db.Connector
	username string
	password string
	url      *url.URL
}

func NewCommand(conf *config.Config) *Command {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	conn, err := db.NewConnectorWithDb(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.Metrics.Database.Name, conf.TDengine.Usessl)
	if err != nil {
		logger.Errorf("init db connect error, msg:%s", err)
		panic(err)
	}

	imp := &Command{
		client:   client,
		conn:     conn,
		username: conf.TDengine.Username,
		password: conf.TDengine.Password,
		url: &url.URL{
			Scheme:   "http",
			Host:     fmt.Sprintf("%s:%d", conf.TDengine.Host, conf.TDengine.Port),
			Path:     "/influxdb/v1/write",
			RawQuery: fmt.Sprintf("db=%s&precision=ms", conf.Metrics.Database.Name),
		},
	}
	return imp
}

func (cmd *Command) Process(conf *config.Config) {
	if len(conf.Transfer) > 0 && len(conf.Drop) > 0 {
		logger.Errorf("transfer and drop can't be set at the same time")
		return
	}

	if len(conf.Transfer) > 0 && conf.Transfer != "old_taosd_metric" {
		logger.Errorf("transfer only support old_taosd_metric")
		return
	}

	if conf.Transfer == "old_taosd_metric" {
		cmd.ProcessTransfer(conf)
		return
	}

	if len(conf.Drop) > 0 && conf.Drop != "old_taosd_metric_stables" {
		logger.Errorf("drop only support old_taosd_metric_stables")
		return
	}

	if conf.Drop == "old_taosd_metric_stables" {
		cmd.ProcessDrop(conf)
		return
	}
}

func (cmd *Command) ProcessTransfer(conf *config.Config) {
	fromTime, err := time.Parse("2006-01-02T15:04:05Z07:00", conf.FromTime)
	if err != nil {
		logger.Errorf("parse fromTime error, msg:%s", err)
		return
	}
	cmd.fromTime = fromTime

	funcs := []func() error{
		cmd.TransferTaosdClusterBasicInfo,
		cmd.TransferTaosdClusterInfo,
		cmd.TransferTaosdVgroupsInfo,
		cmd.TransferTaosdDnodesInfo,
		cmd.TransferTaosdDnodesStatus,
		cmd.TransferTaosdDnodesLogDirs1,
		cmd.TransferTaosdDnodesLogDirs2,
		cmd.TransferTaosdDnodesDataDirs,
		cmd.TransferTaosdMnodesInfo,
		cmd.TransferTaosdVnodesInfo,
	}
	wg := sync.WaitGroup{}
	wg.Add(len(funcs))

	for i := range funcs {
		index := i
		err := pool.GoroutinePool.Submit(func() {
			defer wg.Done()
			funcs[index]()
		})

		if err != nil {
			panic(err)
		}
	}

	wg.Wait()
	logger.Info("transfer all old taosd metric success!!")
}

func (cmd *Command) TransferTaosdClusterInfo() error {
	sql := "select a.cluster_id, master_uptime * 3600 * 24 as cluster_uptime, dbs_total, tbs_total, stbs_total, dnodes_total, dnodes_alive, mnodes_total, mnodes_alive, vgroups_total, vgroups_alive, vnodes_total, vnodes_alive, connections_total, topics_total, streams_total, b.expire_time as grants_expire_time, b.timeseries_used as grants_timeseries_used, b.timeseries_total as grants_timeseries_total, a.ts from cluster_info a, grants_info b where a.ts = b.ts and a.cluster_id = b.cluster_id and"
	dstTable := "taosd_cluster_info"
	return cmd.TransferTableToDst(sql, dstTable, 1)
}

func (cmd *Command) TransferTaosdVgroupsInfo() error {
	sql := "select cluster_id, vgroup_id, database_name, tables_num, CASE status WHEN 'ready' THEN 1 ELSE 0 END as status, ts from vgroups_info a where "
	dstTable := "taosd_vgroups_info"
	return cmd.TransferTableToDst(sql, dstTable, 3)
}

func (cmd *Command) TransferTaosdDnodesInfo() error {
	sql := "select a.cluster_id, a.dnode_id, a.dnode_ep, uptime  * 3600 * 24 as uptime, cpu_engine, cpu_system, cpu_cores, mem_engine, mem_system as mem_free, mem_total, disk_used, disk_total, disk_engine, net_in as system_net_in, net_out as system_net_out, io_read, io_write, io_read_disk, io_write_disk, vnodes_num, masters, has_mnode, has_qnode, has_snode, has_bnode, errors, b.error as error_log_count, b.info as info_log_count, b.debug as debug_log_count, b.trace as trace_log_count, a.ts as ts from dnodes_info a, log_summary b where a.ts = b.ts and a.dnode_id = b.dnode_id and a. dnode_ep = b.dnode_ep and "
	dstTable := "taosd_dnodes_info"
	return cmd.TransferTableToDst(sql, dstTable, 3)
}
func (cmd *Command) TransferTaosdDnodesStatus() error {
	sql := "select cluster_id, dnode_id, dnode_ep, CASE status WHEN 'ready' THEN 1 ELSE 0 END as status, ts from d_info a where "
	dstTable := "taosd_dnodes_status"
	return cmd.TransferTableToDst(sql, dstTable, 3)
}

func (cmd *Command) TransferTaosdDnodesLogDirs1() error {
	sql := "select cluster_id, dnode_id, dnode_ep, name as log_dir_name, avail, used, total, ts from log_dir a where "
	dstTable := "taosd_dnodes_log_dirs"
	return cmd.TransferTableToDst(sql, dstTable, 4)
}
func (cmd *Command) TransferTaosdDnodesLogDirs2() error {
	sql := "select cluster_id, dnode_id, dnode_ep, name as log_dir_name, avail, used, total, ts from temp_dir a where "
	dstTable := "taosd_dnodes_log_dirs"
	return cmd.TransferTableToDst(sql, dstTable, 4)
}

func (cmd *Command) TransferTaosdDnodesDataDirs() error {
	sql := "select cluster_id, dnode_id, dnode_ep, name as data_dir_name, `level` as data_dir_level, avail, used, total, ts from data_dir a where "
	dstTable := "taosd_dnodes_data_dirs"
	return cmd.TransferTableToDst(sql, dstTable, 5)
}

func (cmd *Command) TransferTaosdMnodesInfo() error {
	sql := "select cluster_id, mnode_id, mnode_ep, CASE role WHEN 'offline' THEN 0 WHEN 'follower' THEN 100 WHEN 'candidate' THEN 101 WHEN 'leader' THEN 102 WHEN 'learner' THEN 104 ELSE 103 END as role, ts from m_info a where "
	dstTable := "taosd_mnodes_info"
	return cmd.TransferTableToDst(sql, dstTable, 3)
}

func (cmd *Command) TransferTaosdVnodesInfo() error {
	sql := "select cluster_id, 0 as vgroup_id, 'UNKNOWN' as database_name, dnode_id, CASE vnode_role WHEN 'offline' THEN 0 WHEN 'follower' THEN 100 WHEN 'candidate' THEN 101 WHEN 'leader' THEN 102 WHEN 'learner' THEN 104 ELSE 103 END as role, ts from vnodes_role a where "
	dstTable := "taosd_vnodes_info"
	return cmd.TransferTableToDst(sql, dstTable, 4)
}

func (cmd *Command) ProcessDrop(conf *config.Config) {
	var dropStableList = []string{
		"log_dir",
		"dnodes_info",
		"data_dir",
		"log_summary",
		"m_info",
		"vnodes_role",
		"cluster_info",
		"temp_dir",
		"grants_info",
		"vgroups_info",
		"d_info",
		"taosadapter_system_cpu_percent",
		"taosadapter_restful_http_request_in_flight",
		"taosadapter_restful_http_request_summary_milliseconds",
		"taosadapter_restful_http_request_fail",
		"taosadapter_system_mem_percent",
		"taosadapter_restful_http_request_total",
	}
	ctx := context.Background()
	logger.Infof("use database:%s", conf.Metrics.Database.Name)

	for _, stable := range dropStableList {
		if _, err := cmd.conn.Exec(ctx, "DROP STABLE IF EXISTS "+stable, util.GetQidOwn()); err != nil {
			logger.Errorf("drop stable %s, error:%s", stable, err)
			panic(err)
		}
	}
	logger.Info("drop old taosd metric stables success!!")
}

func (cmd *Command) TransferDataToDest(data *db.Data, dstTable string, tagNum int) {

	var buf bytes.Buffer

	if len(data.Data) < 1 {
		return
	}

	for _, row := range data.Data {
		// get one row here
		buf.WriteString(dstTable)

		// write tags
		var tag string
		for j := 0; j < tagNum; j++ {
			switch v := row[j].(type) {
			case int:
				tag = fmt.Sprint(v)
			case int32:
				tag = fmt.Sprint(v)
			case int64:
				tag = fmt.Sprint(v)
			case string:
				tag = v
			default:
				panic(fmt.Sprintf("Unexpected type for row[%d]: %T", j, row[j]))
			}

			if tag != "" {
				buf.WriteString(fmt.Sprintf(",%s=%s", data.Head[j], util.EscapeInfluxProtocol(tag)))
			} else {
				buf.WriteString(fmt.Sprintf(",%s=%s", data.Head[j], "unknown"))
				logger.Errorf("tag value is empty, tag_name:%s", data.Head[j])
			}
		}
		buf.WriteString(" ")

		// write metrics
		for j := tagNum; j < len(row)-1; j++ {

			switch v := row[j].(type) {
			case int:
				buf.WriteString(fmt.Sprintf("%s=%ff64", data.Head[j], float64(v)))
			case int32:
				buf.WriteString(fmt.Sprintf("%s=%ff64", data.Head[j], float64(v)))
			case int64:
				buf.WriteString(fmt.Sprintf("%s=%ff64", data.Head[j], float64(v)))
			case float32:
				buf.WriteString(fmt.Sprintf("%s=%sf64", data.Head[j], strconv.FormatFloat(float64(v), 'f', -1, 64)))
			case float64:
				buf.WriteString(fmt.Sprintf("%s=%sf64", data.Head[j], strconv.FormatFloat(v, 'f', -1, 64)))
			default:
				panic(fmt.Sprintf("Unexpected type for row[%d]: %T", j, row[j]))
			}

			if j != len(row)-2 {
				buf.WriteString(",")
			}
		}

		// write timestamp
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprintf("%v", row[len(row)-1].(time.Time).UnixMilli()))
		buf.WriteString("\n")

		if buf.Len() >= MAX_SQL_LEN {
			if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
				logger.Tracef("buf:%v", buf.String())
			}
			err := cmd.lineWriteBody(&buf)
			if err != nil {
				logger.Errorf("insert data error, msg:%s", err)
				panic(err)
			}
			buf.Reset()
		}
	}

	if buf.Len() > 0 {
		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			logger.Tracef("buf:%v", buf.String())
		}
		err := cmd.lineWriteBody(&buf)
		if err != nil {
			logger.Errorf("insert data error, msg:%s", err)
			panic(err)
		}
	}
}

// cluster_info
func (cmd *Command) TransferTaosdClusterBasicInfo() error {

	ctx := context.Background()

	endTime := time.Now()
	delta := time.Hour * 24 * 10

	var createTableSql = "create stable if not exists taosd_cluster_basic " +
		"(ts timestamp, first_ep varchar(100), first_ep_dnode_id INT, cluster_version varchar(20)) " +
		"tags (cluster_id varchar(50))"

	if _, err := cmd.conn.Exec(ctx, createTableSql, util.GetQidOwn()); err != nil {
		logger.Errorf("create taosd_cluster_basic error, msg:%s", err)
		return err
	}

	logger.Tracef("fromeTime:%d", cmd.fromTime.UnixMilli())

	for current := cmd.fromTime; current.Before(endTime); current = current.Add(time.Duration(delta)) {
		querySql := fmt.Sprintf("select cluster_id, first_ep, first_ep_dnode_id, `version` as cluster_version, ts from cluster_info where ts > %d and ts <= %d",
			current.UnixMilli(), current.Add(time.Duration(delta)).UnixMilli())
		logger.Tracef("query sql:%s", querySql)
		data, err := cmd.conn.Query(ctx, querySql, util.GetQidOwn())
		if err != nil {
			logger.Errorf("query cluster_info error, msg:%s", err)
			return err
		}

		// transfer data to new table, only this table need use insert statement
		var buf bytes.Buffer

		// 使用 map 将二维数组切分为多个二维数组
		result := make(map[string][][]interface{})
		for _, row := range data.Data {
			key := row[0].(string) // 使用第一列的值作为 key
			result[key] = append(result[key], row)
		}

		// 按照不同 tag 来迁移数据
		for _, dataByCluster := range result {
			buf.Reset()

			for _, row := range dataByCluster {
				if len(buf.Bytes()) == 0 {
					sql := fmt.Sprintf(
						"insert into taosd_cluster_basic_%s using taosd_cluster_basic tags ('%s') values ",
						row[0].(string), row[0].(string))

					buf.WriteString(sql)
				}

				sql := fmt.Sprintf(
					"(%d, '%s', %d, '%s')",
					row[4].(time.Time).UnixMilli(), row[1].(string), row[2].(int32), row[3].(string))
				buf.WriteString(sql)

				if buf.Len() >= MAX_SQL_LEN {
					rowsAffected, err := cmd.conn.Exec(context.Background(), buf.String(), util.GetQidOwn())
					if err != nil {
						logger.Errorf("insert taosd_cluster_basic error, msg:%s", err)
						return err
					}
					if rowsAffected <= 0 {
						logger.Errorf("insert taosd_cluster_basic failed, rowsAffected:%d", rowsAffected)
					}
					buf.Reset()
				}
			}

			if buf.Len() > 0 {
				rowsAffected, err := cmd.conn.Exec(context.Background(), buf.String(), util.GetQidOwn())
				if err != nil {
					logger.Errorf("insert taosd_cluster_basic error, msg:%s", err)
					return err
				}
				if rowsAffected <= 0 {
					logger.Errorf("insert taosd_cluster_basic failed, rowsAffected:%d", rowsAffected)
				}
			}
		}
	}

	logger.Info("transfer stable taosd_cluster_basic success!!")
	return nil
}

// cluster_info
func (cmd *Command) TransferTableToDst(sql string, dstTable string, tagNum int) error {

	ctx := context.Background()

	endTime := time.Now()
	delta := time.Hour * 24 * 10

	logger.Tracef("fromTime:%d", cmd.fromTime.UnixMilli())

	for current := cmd.fromTime; current.Before(endTime); current = current.Add(time.Duration(delta)) {
		querySql := fmt.Sprintf(sql+" a.ts > %d and a.ts <= %d",
			current.UnixMilli(), current.Add(time.Duration(delta)).UnixMilli())
		logger.Tracef("query sql:%s", querySql)
		data, err := cmd.conn.Query(ctx, querySql, util.GetQidOwn())
		if err != nil {
			logger.Errorf("query cluster_info error, msg:%s", err)
			return err
		}

		// transfer data to new table, only this table need use insert statement
		cmd.TransferDataToDest(data, dstTable, tagNum)
	}

	logger.Info("transfer stable " + dstTable + " success!!")
	return nil
}

func (cmd *Command) lineWriteBody(buf *bytes.Buffer) error {
	header := map[string][]string{
		"Connection": {"keep-alive"},
	}

	req := &http.Request{
		Method:     http.MethodPost,
		URL:        cmd.url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Host:       cmd.url.Host,
	}
	req.SetBasicAuth(cmd.username, cmd.password)

	req.Body = io.NopCloser(buf)
	resp, err := cmd.client.Do(req)

	if err != nil {
		logger.Errorf("writing metrics exception, msg:%s", err)
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d:body:%s", resp.StatusCode, string(body))
	}
	return nil
}
