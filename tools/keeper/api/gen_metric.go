package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"

	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

var re = regexp.MustCompile("'+")
var gmLogger = log.GetLogger("GEN")

var MAX_SQL_LEN = 1000000

var STABLE_NAME_KEY = "priv_stn"

type ColumnSeq struct {
	tagNames    []string
	metricNames []string
}

var (
	mu            sync.RWMutex
	gColumnSeqMap = make(map[string]ColumnSeq)
)

type GeneralMetric struct {
	client   *http.Client
	conn     *db.Connector
	username string
	password string
	host     string
	port     int
	usessl   bool
	database string
	url      *url.URL
}

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

type MetricGroup struct {
	Tags    []Tag    `json:"tags"`
	Metrics []Metric `json:"metrics"`
}

type StableInfo struct {
	Name         string        `json:"name"`
	MetricGroups []MetricGroup `json:"metric_groups"`
}

type StableArrayInfo struct {
	Ts       string       `json:"ts"`
	Protocol int          `json:"protocol"`
	Tables   []StableInfo `json:"tables"`
}

type ClusterBasic struct {
	ClusterId      string `json:"cluster_id"`
	Ts             string `json:"ts"`
	FirstEp        string `json:"first_ep"`
	FirstEpDnodeId int32  `json:"first_ep_dnode_id"`
	ClusterVersion string `json:"cluster_version"`
}

type SlowSqlDetailInfo struct {
	StartTs     string `json:"start_ts"`
	RequestId   string `json:"request_id"`
	QueryTime   int32  `json:"query_time"`
	Code        int32  `json:"code"`
	ErrorInfo   string `json:"error_info"`
	Type        int8   `json:"type"`
	RowsNum     int64  `json:"rows_num"`
	Sql         string `json:"sql"`
	ProcessName string `json:"process_name"`
	ProcessId   string `json:"process_id"`
	Db          string `json:"db"`
	User        string `json:"user"`
	Ip          string `json:"ip"`
	ClusterId   string `json:"cluster_id"`
}

func (gm *GeneralMetric) Init(c gin.IRouter) error {
	c.POST("/general-metric", gm.handleFunc())
	c.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())
	c.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	conn, err := db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	if err != nil {
		gmLogger.Errorf("init db connect error, msg:%s", err)
		return err
	}
	gm.conn = conn

	err = gm.createSTables()
	if err != nil {
		gmLogger.Errorf("create stable error, msg:%s", err)
		return err
	}

	err = gm.initColumnSeqMap()
	if err != nil {
		gmLogger.Errorf("init  gColumnSeqMap error, msg:%s", err)
		return err
	}

	return err
}

func NewGeneralMetric(conf *config.Config) *GeneralMetric {

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

	var protocol string
	if conf.TDengine.Usessl {
		protocol = "https"
	} else {
		protocol = "http"
	}

	imp := &GeneralMetric{
		client:   client,
		username: conf.TDengine.Username,
		password: conf.TDengine.Password,
		host:     conf.TDengine.Host,
		port:     conf.TDengine.Port,
		usessl:   conf.TDengine.Usessl,
		database: conf.Metrics.Database.Name,
		url: &url.URL{
			Scheme:   protocol,
			Host:     fmt.Sprintf("%s:%d", conf.TDengine.Host, conf.TDengine.Port),
			Path:     "/influxdb/v1/write",
			RawQuery: fmt.Sprintf("db=%s&precision=ms&table_name_key=%s", conf.Metrics.Database.Name, STABLE_NAME_KEY),
		},
	}
	return imp
}

func (gm *GeneralMetric) handleFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		gmLogger := gmLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		if gm.client == nil {
			gmLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			gmLogger.Errorf("get general metric data error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get general metric data error. %s", err)})
			return
		}

		var request []StableArrayInfo

		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Tracef("data:%s", string(data))
		}

		if err := json.Unmarshal(data, &request); err != nil {
			gmLogger.Errorf("parse general metric data error, data:%s, error:%s", string(data), err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse general metric data error: %s", err)})
			return
		}

		if len(request) == 0 {
			c.JSON(http.StatusOK, gin.H{})
			return
		}

		err = gm.handleBatchMetrics(request, qid)

		if err != nil {
			gmLogger.Errorf("process records error. msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("process records error. %s", err)})
			return
		}

		c.JSON(http.StatusOK, gin.H{})
	}
}

func (gm *GeneralMetric) handleBatchMetrics(request []StableArrayInfo, qid uint64) error {
	var buf bytes.Buffer

	for _, stableArrayInfo := range request {
		if stableArrayInfo.Ts == "" {
			gmLogger.Error("ts data is empty")
			continue
		}

		for _, table := range stableArrayInfo.Tables {
			if table.Name == "" {
				gmLogger.Error("stable name is empty")
				continue
			}

			table.Name = strings.ToLower(table.Name)
			if _, ok := Load(table.Name); !ok {
				Init(table.Name)
			}

			for _, metricGroup := range table.MetricGroups {
				buf.WriteString(table.Name)
				writeTags(metricGroup.Tags, table.Name, &buf)
				buf.WriteString(" ")
				writeMetrics(metricGroup.Metrics, table.Name, &buf)
				buf.WriteString(" ")
				buf.WriteString(stableArrayInfo.Ts)
				buf.WriteString("\n")
			}
		}
	}

	if buf.Len() > 0 {
		return gm.lineWriteBody(&buf, qid)
	}
	return nil
}

func (gm *GeneralMetric) lineWriteBody(buf *bytes.Buffer, qid uint64) error {
	gmLogger := gmLogger.WithFields(
		logrus.Fields{config.ReqIDKey: qid},
	)

	header := map[string][]string{
		"Connection": {"keep-alive"},
	}
	req_data := buf.String()

	//build new URL，add qid to URL
	urlWithQid := *gm.url
	query := urlWithQid.Query()
	query.Set("qid", fmt.Sprintf("%d", qid))
	urlWithQid.RawQuery = query.Encode()

	req := &http.Request{
		Method:     http.MethodPost,
		URL:        &urlWithQid,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Host:       gm.url.Host,
	}
	req.SetBasicAuth(gm.username, gm.password)

	req.Body = io.NopCloser(buf)

	startTime := time.Now()
	resp, err := gm.client.Do(req)

	endTime := time.Now()
	latency := endTime.Sub(startTime)

	if err != nil {
		gmLogger.Errorf("latency:%v, req_data:%v, url:%s, resp:%d, err:%s", latency, req_data, urlWithQid.String(), resp.StatusCode, err)
		return err
	}
	if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
		gmLogger.Tracef("latency:%v, req_data:%v, url:%s, resp:%d", latency, req_data, urlWithQid.String(), resp.StatusCode)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d:body:%s", resp.StatusCode, string(body))
	}
	return nil
}

func (gm *GeneralMetric) handleTaosdClusterBasic() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		gmLogger := gmLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		if gm.conn == nil {
			gmLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			gmLogger.Errorf("get taosd cluster basic data error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get general metric data error. %s", err)})
			return
		}
		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Tracef("receive taosd cluster basic data:%s", string(data))
		}

		var request ClusterBasic

		if err := json.Unmarshal(data, &request); err != nil {
			gmLogger.Errorf("parse general metric data error, data:%s, msg:%s", string(data), err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse general metric data error: %s", err)})
			return
		}

		sql := fmt.Sprintf(
			"insert into %s.taosd_cluster_basic_%s using taosd_cluster_basic tags ('%s') values (%s, '%s', %d, '%s') ",
			gm.database, request.ClusterId, request.ClusterId, request.Ts, request.FirstEp, request.FirstEpDnodeId, request.ClusterVersion)

		if _, err = gm.conn.Exec(context.Background(), sql, qid); err != nil {
			gmLogger.Errorf("insert taosd_cluster_basic error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("insert taosd_cluster_basic error. %s", err)})
			return
		}
		c.JSON(http.StatusOK, gin.H{})
	}
}

func processString(input string) string {
	// remove number in the beginning
	re := regexp.MustCompile(`^\d+`)
	input = re.ReplaceAllString(input, "")

	// replage "."  to "_"
	input = strings.ReplaceAll(input, ".", "_")

	//  remove special characters
	re = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	input = re.ReplaceAllString(input, "")

	return input
}

func (gm *GeneralMetric) handleSlowSqlDetailBatch() gin.HandlerFunc {
	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		gmLogger := gmLogger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)

		if gm.conn == nil {
			gmLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			gmLogger.Errorf("get taos slow sql detail data error, msg:%s", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get taos slow sql detail data error. %s", err)})
			return
		}
		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Tracef("receive taos slow sql detail data:%s", string(data))
		}

		var request []SlowSqlDetailInfo

		if err := json.Unmarshal(data, &request); err != nil {
			gmLogger.Errorf("parse taos slow sql detail error, msg:%s", string(data))
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse taos slow sql detail error: %s", err)})
			return
		}

		var sql_head = "INSERT INTO `taos_slow_sql_detail` (tbname, `db`, `user`, `ip`, `cluster_id`, `start_ts`, `request_id`, `query_time`, `code`, `error_info`, `type`, `rows_num`, `sql`, `process_name`, `process_id`) values "
		var buf bytes.Buffer
		buf.WriteString(sql_head)
		var qid_counter uint8 = 0
		for _, slowSqlDetailInfo := range request {
			if slowSqlDetailInfo.StartTs == "" {
				gmLogger.Error("start_ts data is empty")
				continue
			}

			// cut string to max len
			slowSqlDetailInfo.Sql = re.ReplaceAllString(slowSqlDetailInfo.Sql, "'") // 将匹配到的部分替换为一个单引号
			slowSqlDetailInfo.Sql = strings.ReplaceAll(slowSqlDetailInfo.Sql, "'", "''")
			slowSqlDetailInfo.Sql = util.SafeSubstring(slowSqlDetailInfo.Sql, 16384)
			slowSqlDetailInfo.ClusterId = util.SafeSubstring(slowSqlDetailInfo.ClusterId, 32)
			slowSqlDetailInfo.Db = util.SafeSubstring(slowSqlDetailInfo.Db, 1024)
			if slowSqlDetailInfo.Db == "" {
				slowSqlDetailInfo.Db = "unknown"
			}
			slowSqlDetailInfo.User = util.SafeSubstring(slowSqlDetailInfo.User, 32)
			slowSqlDetailInfo.Ip = util.SafeSubstring(slowSqlDetailInfo.Ip, 32)
			slowSqlDetailInfo.ProcessName = util.SafeSubstring(slowSqlDetailInfo.ProcessName, 32)
			slowSqlDetailInfo.ProcessId = util.SafeSubstring(slowSqlDetailInfo.ProcessId, 32)
			slowSqlDetailInfo.ErrorInfo = util.SafeSubstring(slowSqlDetailInfo.ErrorInfo, 128)

			// max len 192
			var sub_table_name = slowSqlDetailInfo.User + "_" + util.SafeSubstring(slowSqlDetailInfo.Db, 80) + "_" + slowSqlDetailInfo.Ip + "_clusterId_" + slowSqlDetailInfo.ClusterId
			sub_table_name = strings.ToLower(processString(sub_table_name))

			var sql = fmt.Sprintf(
				"('%s', '%s', '%s', '%s', '%s', %s, %s, %d, %d, '%s', %d, %d, '%s', '%s', '%s') ",
				sub_table_name,
				slowSqlDetailInfo.Db, slowSqlDetailInfo.User, slowSqlDetailInfo.Ip, slowSqlDetailInfo.ClusterId, slowSqlDetailInfo.StartTs, slowSqlDetailInfo.RequestId,
				slowSqlDetailInfo.QueryTime, slowSqlDetailInfo.Code, slowSqlDetailInfo.ErrorInfo, slowSqlDetailInfo.Type, slowSqlDetailInfo.RowsNum, slowSqlDetailInfo.Sql,
				slowSqlDetailInfo.ProcessName, slowSqlDetailInfo.ProcessId)
			if (buf.Len() + len(sql)) < MAX_SQL_LEN {
				buf.WriteString(sql)
			} else {
				if _, err = gm.conn.Exec(context.Background(), buf.String(), qid|uint64((qid_counter%255))); err != nil {
					gmLogger.Errorf("insert taos_slow_sql_detail error, sql:%s, error:%s", buf.String(), err)
					c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("insert taos_slow_sql_detail error. %s", err)})
					return
				}
				buf.Reset()
				buf.WriteString(sql_head)
				buf.WriteString(sql)
				qid_counter++
			}
		}

		if buf.Len() > len(sql_head) {
			if _, err = gm.conn.Exec(context.Background(), buf.String(), qid|uint64((qid_counter%255))); err != nil {
				gmLogger.Errorf("insert taos_slow_sql_detail error, data:%s, msg:%s", buf.String(), err)
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("insert taos_slow_sql_detail error. %s", err)})
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{})
	}
}

func writeTags(tags []Tag, stbName string, buf *bytes.Buffer) {
	var nameArray []string
	if columnSeq, ok := Load(stbName); ok {
		if len(columnSeq.tagNames) < len(tags) {
			// add column, only schema change will hit here
			for _, tag := range tags {
				if !contains(columnSeq.tagNames, tag.Name) {
					columnSeq.tagNames = append(columnSeq.tagNames, tag.Name)
				}
			}
			Store(stbName, columnSeq)
		}
		nameArray = columnSeq.tagNames
	}

	// 将 Tag 切片转换为 map
	tagMap := make(map[string]string)
	for _, tag := range tags {
		tagMap[tag.Name] = tag.Value
	}

	for _, name := range nameArray {
		if value, ok := tagMap[name]; ok {
			if value != "" {
				buf.WriteString(fmt.Sprintf(",%s=%s", name, util.EscapeInfluxProtocol(value)))
			} else {
				buf.WriteString(fmt.Sprintf(",%s=%s", name, "unknown"))
				gmLogger.Errorf("tag value is empty, tag name:%s", name)
			}
		} else {
			buf.WriteString(fmt.Sprintf(",%s=%s", name, "unknown"))
		}
	}

	// have sub table name
	if _, ok := tagMap[STABLE_NAME_KEY]; ok {
		return
	}

	subTableName := get_sub_table_name_valid(stbName, tagMap)
	if subTableName != "" {
		buf.WriteString(fmt.Sprintf(",%s=%s", STABLE_NAME_KEY, subTableName))
	} else {
		gmLogger.Errorf("get sub stable name error, stable name:%s, tag map:%v", stbName, tagMap)
	}
}

func checkKeysExist(data map[string]string, keys ...string) bool {
	for _, key := range keys {
		_, ok := data[key]
		if !ok {
			return false
		}
	}
	return true
}

func get_sub_table_name_valid(stbName string, tagMap map[string]string) string {
	stbName = get_sub_table_name(stbName, tagMap)
	return util.ToValidTableName(stbName)
}

func get_sub_table_name(stbName string, tagMap map[string]string) string {
	if strings.HasPrefix(stbName, "taosx") {
		switch stbName {
		case "taosx_sys":
			if checkKeysExist(tagMap, "taosx_id") {
				return fmt.Sprintf("sys_%s", tagMap["taosx_id"])
			}
		case "taosx_agent":
			if checkKeysExist(tagMap, "taosx_id", "agent_id") {
				return fmt.Sprintf("agent_%s_%s", tagMap["taosx_id"], tagMap["agent_id"])
			}
		case "taosx_connector":
			if checkKeysExist(tagMap, "taosx_id", "ds_name", "task_id") {
				return fmt.Sprintf("connector_%s_%s_%s", tagMap["taosx_id"], tagMap["ds_name"], tagMap["task_id"])
			}
		default:
			if strings.HasPrefix(stbName, "taosx_task_") {
				ds_name := stbName[len("taosx_task_"):]
				if checkKeysExist(tagMap, "taosx_id", "task_id") {
					return fmt.Sprintf("task_%s_%s_%s", tagMap["taosx_id"], ds_name, tagMap["task_id"])
				}
			}
			return ""
		}
	}

	switch stbName {
	case "taosd_cluster_info":
		if checkKeysExist(tagMap, "cluster_id") {
			return fmt.Sprintf("cluster_%s", tagMap["cluster_id"])
		}
	case "taosd_vgroups_info":
		if checkKeysExist(tagMap, "cluster_id", "vgroup_id", "database_name") {
			return fmt.Sprintf("vginfo_%s_vgroup_%s_cluster_%s", tagMap["database_name"], tagMap["vgroup_id"], tagMap["cluster_id"])
		}
	case "taosd_dnodes_info":
		if checkKeysExist(tagMap, "cluster_id", "dnode_id") {
			return fmt.Sprintf("dinfo_%s_cluster_%s", tagMap["dnode_id"], tagMap["cluster_id"])
		}
	case "taosd_dnodes_status":
		if checkKeysExist(tagMap, "cluster_id", "dnode_id") {
			return fmt.Sprintf("dstatus_%s_cluster_%s", tagMap["dnode_id"], tagMap["cluster_id"])
		}
	case "taosd_dnodes_log_dirs":
		if checkKeysExist(tagMap, "cluster_id", "dnode_id", "data_dir_name") {
			subTableName := fmt.Sprintf("dlog_%s_%s_cluster_%s", tagMap["dnode_id"], tagMap["data_dir_name"], tagMap["cluster_id"])
			if len(subTableName) <= util.MAX_TABLE_NAME_LEN {
				return subTableName
			}
			return fmt.Sprintf("dlog_%s_%s_cluster_%s", tagMap["dnode_id"],
				util.GetMd5HexStr(tagMap["data_dir_name"]),
				tagMap["cluster_id"])
		}
	case "taosd_dnodes_data_dirs":
		if checkKeysExist(tagMap, "cluster_id", "dnode_id", "data_dir_name", "data_dir_level") {
			subTableName := fmt.Sprintf("ddata_%s_%s_level_%s_cluster_%s", tagMap["dnode_id"], tagMap["data_dir_name"], tagMap["data_dir_level"], tagMap["cluster_id"])
			if len(subTableName) <= util.MAX_TABLE_NAME_LEN {
				return subTableName
			}
			return fmt.Sprintf("ddata_%s_%s_level_%s_cluster_%s", tagMap["dnode_id"],
				util.GetMd5HexStr(tagMap["data_dir_name"]),
				tagMap["data_dir_level"],
				tagMap["cluster_id"])
		}
	case "taosd_mnodes_info":
		if checkKeysExist(tagMap, "cluster_id", "mnode_id") {
			return fmt.Sprintf("minfo_%s_cluster_%s", tagMap["mnode_id"], tagMap["cluster_id"])
		}
	case "taosd_vnodes_info":
		if checkKeysExist(tagMap, "cluster_id", "database_name", "vgroup_id", "dnode_id") {
			return fmt.Sprintf("vninfo_%s_dnode_%s_vgroup_%s_cluster_%s", tagMap["database_name"], tagMap["dnode_id"], tagMap["vgroup_id"], tagMap["cluster_id"])
		}
	case "taosd_sql_req":
		if checkKeysExist(tagMap, "username", "sql_type", "result", "dnode_id", "vgroup_id", "cluster_id") {
			return fmt.Sprintf("taosdsql_%s_%s_%s_%s_vgroup_%s_cluster_%s", tagMap["username"],
				tagMap["sql_type"], tagMap["result"], tagMap["dnode_id"], tagMap["vgroup_id"], tagMap["cluster_id"])
		}
	case "taos_sql_req":
		if checkKeysExist(tagMap, "username", "sql_type", "result", "cluster_id") {
			return fmt.Sprintf("taossql_%s_%s_%s_cluster_%s", tagMap["username"],
				tagMap["sql_type"], tagMap["result"], tagMap["cluster_id"])
		}
	case "taos_slow_sql":
		if checkKeysExist(tagMap, "username", "duration", "result", "cluster_id") {
			return fmt.Sprintf("slowsql_%s_%s_%s_cluster_%s", tagMap["username"],
				tagMap["duration"], tagMap["result"], tagMap["cluster_id"])
		}

	default:
		return ""
	}
	return ""
}

func contains(array []string, item string) bool {
	for _, value := range array {
		if value == item {
			return true
		}
	}
	return false
}

func writeMetrics(metrics []Metric, stbName string, buf *bytes.Buffer) {
	var nameArray []string
	if columnSeq, ok := Load(stbName); ok {
		if len(columnSeq.metricNames) < len(metrics) {
			// add column, only schema change will hit here
			for _, metric := range metrics {
				if !contains(columnSeq.metricNames, metric.Name) {
					columnSeq.metricNames = append(columnSeq.metricNames, metric.Name)
				}
			}
			Store(stbName, columnSeq)
		}
		nameArray = columnSeq.metricNames
	}

	// 将 Metric 切片转换为 map
	metricMap := make(map[string]float64)
	for _, metric := range metrics {
		metricMap[metric.Name] = metric.Value
	}

	for i, name := range nameArray {
		if value, ok := metricMap[name]; ok {
			buf.WriteString(fmt.Sprintf("%s=%sf64", name, strconv.FormatFloat(value, 'f', -1, 64)))
			if i != len(nameArray)-1 {
				buf.WriteString(",")
			}
		}
	}
}

// 存储数据
func Store(key string, value ColumnSeq) {
	mu.Lock()
	defer mu.Unlock()
	gColumnSeqMap[key] = value
}

// 加载数据
func Load(key string) (ColumnSeq, bool) {
	mu.RLock()
	defer mu.RUnlock()
	value, ok := gColumnSeqMap[key]
	return value, ok
}

// 初始化单表的列序列
func Init(key string) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := gColumnSeqMap[key]; !ok {
		columnSeq := ColumnSeq{
			tagNames:    []string{},
			metricNames: []string{},
		}
		gColumnSeqMap[key] = columnSeq
	}
}

// 初始化所有列序列
func (gm *GeneralMetric) initColumnSeqMap() error {
	query := fmt.Sprintf(`
    select stable_name
    from information_schema.ins_stables
    where db_name = '%s'
    and (
        stable_name like 'taos_%%'
        or stable_name like 'taosd_%%'
        or stable_name like 'taosx_%%'
    )
    order by stable_name asc;
	`, gm.database)

	data, err := gm.conn.Query(context.Background(), query, util.GetQidOwn())

	if err != nil {
		return err
	}

	//get all stables, then init gColumnSeqMap
	for _, row := range data.Data {
		stableName := row[0].(string)
		Init(stableName)
	}
	//set gColumnSeqMap with desc stables
	for tableName, columnSeq := range gColumnSeqMap {
		data, err := gm.conn.Query(context.Background(), fmt.Sprintf(`desc %s.%s;`, gm.database, tableName), util.GetQidOwn())

		if err != nil {
			return err
		}

		if len(data.Data) < 1 || len(data.Data[0]) < 4 {
			return fmt.Errorf("desc %s.%s error", gm.database, tableName)
		}

		for i, row := range data.Data {
			if i == 0 {
				continue
			}

			if row[3].(string) == "TAG" {
				columnSeq.tagNames = append(columnSeq.tagNames, row[0].(string))
			} else {
				columnSeq.metricNames = append(columnSeq.metricNames, row[0].(string))
			}
		}
		Store(tableName, columnSeq)
	}

	gmLogger.Infof("gColumnSeqMap:%v", gColumnSeqMap)
	return nil
}

func (gm *GeneralMetric) createSTables() error {
	var createTableSql = "create stable if not exists taosd_cluster_basic " +
		"(ts timestamp, first_ep varchar(100), first_ep_dnode_id INT, cluster_version varchar(20)) " +
		"tags (cluster_id varchar(50))"

	if gm.conn == nil {
		return errNoConnection
	}
	_, err := gm.conn.Exec(context.Background(), createTableSql, util.GetQidOwn())
	if err != nil {
		return err
	}

	createTableSql = "create stable if not exists taos_slow_sql_detail" +
		" (start_ts TIMESTAMP, request_id BIGINT UNSIGNED PRIMARY KEY, query_time INT, code INT, error_info varchar(128), " +
		"type TINYINT, rows_num BIGINT, sql varchar(16384), process_name varchar(32), process_id varchar(32)) " +
		"tags (db varchar(1024), `user` varchar(32), ip varchar(32), cluster_id varchar(32))"

	_, err = gm.conn.Exec(context.Background(), createTableSql, util.GetQidOwn())
	return err
}
