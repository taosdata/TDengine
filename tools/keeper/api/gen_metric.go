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

var gmLogger = log.GetLogger("gen_metric")

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

func (gm *GeneralMetric) Init(c gin.IRouter) error {
	c.POST("/general-metric", gm.handleFunc())
	c.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())

	conn, err := db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	if err != nil {
		gmLogger.Error("## init db connect error", "error", err)
		return err
	}
	gm.conn = conn

	err = gm.createSTables()
	if err != nil {
		gmLogger.Error("## create stable error", "error", err)
		return err
	}

	err = gm.initColumnSeqMap()
	if err != nil {
		gmLogger.Error("## init  gColumnSeqMap error", "error", err)
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
			RawQuery: fmt.Sprintf("db=%s&precision=ms", conf.Metrics.Database.Name),
		},
	}
	return imp
}

func (gm *GeneralMetric) handleFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		if gm.client == nil {
			gmLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			gmLogger.WithError(err).Errorf("## get general metric data error")
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get general metric data error. %s", err)})
			return
		}

		var request []StableArrayInfo

		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Trace("## data: ", string(data))
		}

		if err := json.Unmarshal(data, &request); err != nil {
			gmLogger.WithError(err).Errorf("## parse general metric data %s error", string(data))
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse general metric data error: %s", err)})
			return
		}

		if len(request) == 0 {
			c.JSON(http.StatusOK, gin.H{})
			return
		}

		err = gm.handleBatchMetrics(request)

		if err != nil {
			gmLogger.WithError(err).Errorf("## process records error")
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("process records error. %s", err)})
			return
		}

		c.JSON(http.StatusOK, gin.H{})
	}
}

func (gm *GeneralMetric) handleBatchMetrics(request []StableArrayInfo) error {
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
		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Tracef("## buf: %v", buf.String())
		}
		return gm.lineWriteBody(&buf)
	}
	return nil
}

func (gm *GeneralMetric) lineWriteBody(buf *bytes.Buffer) error {
	header := map[string][]string{
		"Connection": {"keep-alive"},
	}
	req := &http.Request{
		Method:     http.MethodPost,
		URL:        gm.url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Host:       gm.url.Host,
	}
	req.SetBasicAuth(gm.username, gm.password)

	req.Body = io.NopCloser(buf)
	resp, err := gm.client.Do(req)

	if err != nil {
		gmLogger.Errorf("writing metrics exception: %v", err)
		return err
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
		if gm.conn == nil {
			gmLogger.Error("no connection")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "no connection"})
			return
		}

		data, err := c.GetRawData()
		if err != nil {
			gmLogger.WithError(err).Errorf("## get taosd cluster basic data error")
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("get general metric data error. %s", err)})
			return
		}
		if logger.Logger.IsLevelEnabled(logrus.TraceLevel) {
			gmLogger.Trace("## receive taosd cluster basic data: ", string(data))
		}

		var request ClusterBasic

		if err := json.Unmarshal(data, &request); err != nil {
			gmLogger.WithError(err).Errorf("## parse general metric data %s error", string(data))
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("parse general metric data error: %s", err)})
			return
		}

		sql := fmt.Sprintf(
			"insert into %s.taosd_cluster_basic_%s using taosd_cluster_basic tags ('%s') values (%s, '%s', %d, '%s') ",
			gm.database, request.ClusterId, request.ClusterId, request.Ts, request.FirstEp, request.FirstEpDnodeId, request.ClusterVersion)

		if _, err = gm.conn.Exec(context.Background(), sql); err != nil {
			gmLogger.WithError(err).Errorf("## insert taosd_cluster_basic error")
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("insert taosd_cluster_basic error. %s", err)})
			return
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
				gmLogger.Errorf("## tag value is empty, tag name: %s", name)
			}
		} else {
			buf.WriteString(fmt.Sprintf(",%s=%s", name, "unknown"))
		}
	}
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

	data, err := gm.conn.Query(context.Background(), query)

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
		data, err := gm.conn.Query(context.Background(), fmt.Sprintf(`desc %s.%s;`, gm.database, tableName))

		if err != nil {
			return err
		}

		gmLogger.Tracef("## data: %v", data)

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

	gmLogger.Infof("## gColumnSeqMap: %v", gColumnSeqMap)
	return nil
}

func (gm *GeneralMetric) createSTables() error {
	var createTableSql = "create stable if not exists taosd_cluster_basic " +
		"(ts timestamp, first_ep varchar(100), first_ep_dnode_id INT, cluster_version varchar(20)) " +
		"tags (cluster_id varchar(50))"

	if gm.conn == nil {
		return errNoConnection
	}
	_, err := gm.conn.Exec(context.Background(), createTableSql)
	if err != nil {
		return err
	}
	return err
}
