package process

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
	"github.com/taosdata/taoskeeper/util/pool"
)

var logger = log.GetLogger("HND")

var metricNameMap = map[string]string{
	"taosd_cluster_basic_first_ep":          "cluster_info_first_ep",
	"taosd_cluster_basic_first_ep_dnode_id": "cluster_info_first_ep_dnode_id",
	"taosd_cluster_basic_cluster_version":   "cluster_info_version",

	"taosd_cluster_info_cluster_uptime":    "cluster_info_master_uptime",
	"taosd_cluster_info_dbs_total":         "cluster_info_dbs_total",
	"taosd_cluster_info_tbs_total":         "cluster_info_tbs_total",
	"taosd_cluster_info_stbs_total":        "cluster_info_stbs_total",
	"taosd_cluster_info_dnodes_total":      "cluster_info_dnodes_total",
	"taosd_cluster_info_dnodes_alive":      "cluster_info_dnodes_alive",
	"taosd_cluster_info_mnodes_total":      "cluster_info_mnodes_total",
	"taosd_cluster_info_mnodes_alive":      "cluster_info_mnodes_alive",
	"taosd_cluster_info_vgroups_total":     "cluster_info_vgroups_total",
	"taosd_cluster_info_vgroups_alive":     "cluster_info_vgroups_alive",
	"taosd_cluster_info_vnodes_total":      "cluster_info_vnodes_total",
	"taosd_cluster_info_vnodes_alive":      "cluster_info_vnodes_alive",
	"taosd_cluster_info_connections_total": "cluster_info_connections_total",
	"taosd_cluster_info_topics_total":      "cluster_info_topics_total",
	"taosd_cluster_info_streams_total":     "cluster_info_streams_total",

	"taosd_cluster_info_grants_expire_time":      "grants_info_expire_time",
	"taosd_cluster_info_grants_timeseries_used":  "grants_info_timeseries_used",
	"taosd_cluster_info_grants_timeseries_total": "grants_info_timeseries_total",

	"taosd_dnodes_info_uptime":          "dnodes_info_uptime",
	"taosd_dnodes_info_cpu_engine":      "dnodes_info_cpu_engine",
	"taosd_dnodes_info_cpu_system":      "dnodes_info_cpu_system",
	"taosd_dnodes_info_cpu_cores":       "dnodes_info_cpu_cores",
	"taosd_dnodes_info_mem_engine":      "dnodes_info_mem_engine",
	"taosd_dnodes_info_mem_free":        "dnodes_info_mem_system",
	"taosd_dnodes_info_mem_total":       "dnodes_info_mem_total",
	"taosd_dnodes_info_disk_engine":     "dnodes_info_disk_engine",
	"taosd_dnodes_info_disk_used":       "dnodes_info_disk_used",
	"taosd_dnodes_info_disk_total":      "dnodes_info_disk_total",
	"taosd_dnodes_info_system_net_in":   "dnodes_info_net_in",
	"taosd_dnodes_info_system_net_out":  "dnodes_info_net_out",
	"taosd_dnodes_info_io_read":         "dnodes_info_io_read",
	"taosd_dnodes_info_io_write":        "dnodes_info_io_write",
	"taosd_dnodes_info_io_read_disk":    "dnodes_info_io_read_disk",
	"taosd_dnodes_info_io_write_disk":   "dnodes_info_io_write_disk",
	"taosd_dnodes_info_vnodes_num":      "dnodes_info_vnodes_num",
	"taosd_dnodes_info_masters":         "dnodes_info_masters",
	"taosd_dnodes_info_has_mnode":       "dnodes_info_has_mnode",
	"taosd_dnodes_info_has_qnode":       "dnodes_info_has_qnode",
	"taosd_dnodes_info_has_snode":       "dnodes_info_has_snode",
	"taosd_dnodes_info_has_bnode":       "dnodes_info_has_bnode",
	"taosd_dnodes_info_errors":          "dnodes_info_errors",
	"taosd_dnodes_info_error_log_count": "log_summary_error",
	"taosd_dnodes_info_info_log_count":  "log_summary_info",
	"taosd_dnodes_info_debug_log_count": "log_summary_debug",
	"taosd_dnodes_info_trace_log_count": "log_summary_trace",

	"taosd_dnodes_status_status": "d_info_status",

	"taosd_mnodes_info_role": "m_info_role",
}

var metricTypeMap = map[string]CollectType{
	"taosd_cluster_basic_first_ep":          Info,
	"taosd_cluster_basic_first_ep_dnode_id": Counter,
	"taosd_cluster_basic_cluster_version":   Info,

	"taosd_cluster_info_cluster_uptime":    Gauge,
	"taosd_cluster_info_dbs_total":         Counter,
	"taosd_cluster_info_tbs_total":         Counter,
	"taosd_cluster_info_stbs_total":        Counter,
	"taosd_cluster_info_dnodes_total":      Counter,
	"taosd_cluster_info_dnodes_alive":      Counter,
	"taosd_cluster_info_mnodes_total":      Counter,
	"taosd_cluster_info_mnodes_alive":      Counter,
	"taosd_cluster_info_vgroups_total":     Counter,
	"taosd_cluster_info_vgroups_alive":     Counter,
	"taosd_cluster_info_vnodes_total":      Counter,
	"taosd_cluster_info_vnodes_alive":      Counter,
	"taosd_cluster_info_connections_total": Counter,
	"taosd_cluster_info_topics_total":      Counter,
	"taosd_cluster_info_streams_total":     Counter,

	"taosd_cluster_info_grants_expire_time":      Counter,
	"taosd_cluster_info_grants_timeseries_used":  Counter,
	"taosd_cluster_info_grants_timeseries_total": Counter,

	"taosd_dnodes_info_uptime":          Gauge,
	"taosd_dnodes_info_cpu_engine":      Gauge,
	"taosd_dnodes_info_cpu_system":      Gauge,
	"taosd_dnodes_info_cpu_cores":       Gauge,
	"taosd_dnodes_info_mem_engine":      Counter,
	"taosd_dnodes_info_mem_free":        Counter,
	"taosd_dnodes_info_mem_total":       Counter,
	"taosd_dnodes_info_disk_engine":     Counter,
	"taosd_dnodes_info_disk_used":       Counter,
	"taosd_dnodes_info_disk_total":      Counter,
	"taosd_dnodes_info_system_net_in":   Gauge,
	"taosd_dnodes_info_system_net_out":  Gauge,
	"taosd_dnodes_info_io_read":         Gauge,
	"taosd_dnodes_info_io_write":        Gauge,
	"taosd_dnodes_info_io_read_disk":    Gauge,
	"taosd_dnodes_info_io_write_disk":   Gauge,
	"taosd_dnodes_info_vnodes_num":      Counter,
	"taosd_dnodes_info_masters":         Counter,
	"taosd_dnodes_info_has_mnode":       Counter,
	"taosd_dnodes_info_has_qnode":       Counter,
	"taosd_dnodes_info_has_snode":       Counter,
	"taosd_dnodes_info_has_bnode":       Counter,
	"taosd_dnodes_info_errors":          Counter,
	"taosd_dnodes_info_error_log_count": Counter,
	"taosd_dnodes_info_info_log_count":  Counter,
	"taosd_dnodes_info_debug_log_count": Counter,
	"taosd_dnodes_info_trace_log_count": Counter,

	"taosd_dnodes_status_status": Info,

	"taosd_mnodes_info_role": Info,
}

type CollectType string

const (
	Counter CollectType = "counter"
	Gauge   CollectType = "gauge"
	Info    CollectType = "info"
	Summary CollectType = "summary"
)

type Processor struct {
	prefix           string
	db               string
	tableMap         map[string]*Table  //tableName:*Table{}
	metricMap        map[string]*Metric //Fqname:*Metric{}
	tableList        []string
	ctx              context.Context
	rotationInterval time.Duration
	exitChan         chan struct{}
	dbConn           *db.Connector
	summaryTable     map[string]*Table
	tables           map[string]struct{}
}

func (p *Processor) Describe(descs chan<- *prometheus.Desc) {
	for _, metric := range p.metricMap {
		descs <- metric.Desc
	}
}

func (p *Processor) Collect(metrics chan<- prometheus.Metric) {
	for _, metric := range p.metricMap {
		logger.Tracef("metric name:%v", metric.FQName)

		switch metric.Type {
		case Gauge:
			gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name:        metric.FQName,
				Help:        metric.Help,
				ConstLabels: metric.ConstLabels,
			}, metric.Variables)
			for _, value := range metric.GetValue() {
				if value.Value == nil {
					continue
				}
				g := gv.With(value.Label)
				g.Set(value.Value.(float64))
				metrics <- g
			}
		case Counter:
			cv := prometheus.NewCounterVec(prometheus.CounterOpts{
				Name:        metric.FQName,
				Help:        metric.Help,
				ConstLabels: metric.ConstLabels,
			}, metric.Variables)
			for _, value := range metric.GetValue() {
				if value.Value == nil {
					continue
				}
				v := i2float(value.Value)
				if v < 0 {
					logger.Warningf("negative value for prometheus counter. label %v value %v",
						value.Label, value.Value)
					continue
				}
				c := cv.With(value.Label)
				c.Add(v)
				metrics <- c
			}
		case Info:
			lbs := []string{"value"}
			lbs = append(lbs, metric.Variables...)
			gf := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name:        metric.FQName,
				Help:        metric.Help,
				ConstLabels: metric.ConstLabels,
			}, lbs)
			for _, value := range metric.GetValue() {
				if value == nil {
					continue
				}
				v := make(map[string]string, len(value.Label)+1)
				v["value"] = value.Value.(string)
				for k, l := range value.Label {
					v[k] = l
				}
				g := gf.With(v)
				g.Set(1)
				metrics <- g
			}
		case Summary:
		}
	}
}

type Table struct {
	tsName     string
	Variables  []string
	ColumnList []string
}

type Metric struct {
	sync.RWMutex
	FQName      string
	Help        string
	Type        CollectType
	ColType     int
	ConstLabels map[string]string
	Variables   []string
	Desc        *prometheus.Desc
	LastValue   []*Value
}

func (m *Metric) SetValue(v []*Value) {
	m.Lock()
	defer m.Unlock()
	m.LastValue = v
}

func (m *Metric) GetValue() []*Value {
	m.RLock()
	defer m.RUnlock()
	return m.LastValue
}

type Value struct {
	Label map[string]string
	Value interface{}
}

func NewProcessor(conf *config.Config) *Processor {

	conn, err := db.NewConnector(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.TDengine.Usessl)
	if err != nil {
		panic(err)
	}
	interval, err := time.ParseDuration(conf.RotationInterval)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	tables, err := ExpandMetricsFromConfig(ctx, conn, &conf.Metrics)
	if err != nil {
		panic(err)
	}
	p := &Processor{
		prefix:           conf.Metrics.Prefix,
		db:               conf.Metrics.Database.Name,
		tableMap:         map[string]*Table{},
		metricMap:        map[string]*Metric{},
		ctx:              ctx,
		rotationInterval: interval,
		exitChan:         make(chan struct{}),
		dbConn:           conn,
		summaryTable:     map[string]*Table{"taosadapter_restful_http_request_summary_milliseconds": nil},
		tables:           tables,
	}
	p.Prepare()
	p.Process()
	return p
}

func (p *Processor) Prepare() {
	locker := sync.RWMutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(p.tables))

	for tn := range p.tables {
		tableName := tn

		err := pool.GoroutinePool.Submit(func() {
			defer wg.Done()
			data, err := p.dbConn.Query(p.ctx, fmt.Sprintf("describe %s", p.withDBName(tableName)), util.GetQidOwn())
			if err != nil {
				var tdEngineError *taosError.TaosError
				if errors.As(err, &tdEngineError) {
					logger.Errorf("table %s not exist, skip it, error:%s", tableName, err)
				} else {
					logger.Errorf("could not get table %s metadata, skip it, error:%s", tableName, err)
				}
				return
			}

			tags := make([]string, 0, len(data.Data))
			columns := make([]string, 0, len(data.Data))
			typeList := make([]string, 0, len(data.Data))
			columnMap := make(map[string]struct{}, len(data.Data))
			variablesMap := make(map[string]struct{}, len(data.Data))
			for _, info := range data.Data {
				if info[3].(string) != "" {
					variable := info[0].(string)
					tags = append(tags, variable)
					variablesMap[variable] = struct{}{}
				} else {
					column := info[0].(string)
					columns = append(columns, column)
					typeList = append(typeList, info[1].(string))
					columnMap[column] = struct{}{}
				}
			}

			// metrics := make([]*Metric, 0, len(columns))
			// newMetrics := make(map[string]*Metric, len(columns))
			columnList := make([]string, 0, len(columns))

			timestampColumn := "ts"
			_, exist := p.summaryTable[tableName]
			for i, column := range columns {
				if _, columnExist := variablesMap[column]; columnExist {
					continue
				}

				if typeList[i] == "TIMESTAMP" {
					timestampColumn = column
					continue
				}

				columnName, metricType := "", Summary
				if !exist {
					columnName = column

					if _, ok := metricTypeMap[tableName+"_"+columnName]; ok {
						metricType = metricTypeMap[tableName+"_"+columnName]
					} else {
						metricType = exchangeDBType(typeList[i])
					}

					// 为了兼容性，硬编码，后续要优化
					if strings.HasSuffix(columnName, "role") {
						metricType = Info
					}
				}

				labels := make(map[string]string)

				fqName := p.buildFQName(tableName, columnName)
				pDesc := prometheus.NewDesc(fqName, "", nil, labels)
				metric := &Metric{
					Type:        metricType,
					Desc:        pDesc,
					FQName:      fqName,
					Help:        "",
					ConstLabels: labels,
					Variables:   tags,
				}
				// metrics = append(metrics, metric)
				// newMetrics[column] = metric

				locker.Lock()
				p.metricMap[fqName] = metric
				locker.Unlock()

				columnList = append(columnList, column)
			}

			t := &Table{
				tsName:     timestampColumn,
				Variables:  tags,
				ColumnList: columnList,
			}
			locker.Lock()
			p.tableMap[tableName] = t
			p.tableList = append(p.tableList, tableName)
			locker.Unlock()

		})
		if err != nil {
			panic(err)
		}
	}

	wg.Wait()
}

func (p *Processor) withDBName(tableName string) string {
	b := pool.BytesPoolGet()
	b.WriteString(p.db)
	b.WriteByte('.')
	b.WriteString(tableName)
	return b.String()
}

func (p *Processor) Process() {
	// 首先清空所有指标值
	for _, metric := range p.metricMap {
		metric.SetValue(nil)
	}

	for _, tableName := range p.tableList {
		tagIndex := 0
		hasTag := false
		b := pool.BytesPoolGet()
		b.WriteString("select ")

		table := p.tableMap[tableName]
		columns := table.ColumnList

		for i, column := range columns {
			b.WriteString("last_row(`" + column + "`) as `" + column + "`")
			if i != len(columns)-1 {
				b.WriteByte(',')
			}
		}

		if len(table.Variables) > 0 {
			tagIndex = len(columns)
			for _, tag := range table.Variables {
				b.WriteString(", last_row(`" + tag + "`) as `" + tag + "`")
			}
		}

		b.WriteString(" from ")
		b.WriteString(p.withDBName(tableName))

		b.WriteString(" WHERE " + p.tableMap[tableName].tsName + " > (NOW() - 1m) ")

		if len(table.Variables) > 0 {
			tagIndex = len(columns)
			b.WriteString(" group by ")
			for i, tag := range table.Variables {
				b.WriteString("`" + tag + "`")
				if i != len(table.Variables)-1 {
					b.WriteByte(',')
				}
			}
		}
		sql := b.String()
		pool.BytesPoolPut(b)
		data, err := p.dbConn.Query(p.ctx, sql, util.GetQidOwn())
		logger.Debug(sql)
		if err != nil {
			logger.WithError(err).Errorln("select data sql:", sql)
			continue
		}
		if tagIndex > 0 {
			hasTag = true
		}
		if len(data.Data) == 0 {
			continue
		}
		values := make([][]*Value, len(table.ColumnList))
		for _, row := range data.Data {
			label := map[string]string{}
			valuesMap := make(map[string]interface{})
			colEndIndex := len(columns)
			if hasTag {
				for i := tagIndex; i < len(data.Head); i++ {
					if row[i] != nil {
						label[data.Head[i]] = fmt.Sprintf("%v", row[i])
					}
				}
			}
			// values array to map
			for i := 0; i < colEndIndex; i++ {
				valuesMap[columns[i]] = row[i]
			}
			for i, column := range table.ColumnList {
				var v interface{}
				metric := p.metricMap[p.buildFQName(tableName, column)]
				switch metric.Type {
				case Info:
					_, isFloat := valuesMap[column].(float64)
					if strings.HasSuffix(column, "role") && valuesMap[column] != nil && isFloat {
						v = getRoleStr(valuesMap[column].(float64))
						break
					}
					if strings.HasSuffix(column, "status") && valuesMap[column] != nil && isFloat {
						v = getStatusStr(valuesMap[column].(float64))
						break
					}

					if valuesMap[column] != nil {
						v = i2string(valuesMap[column])
					} else {
						v = nil
					}
				case Counter, Gauge, Summary:
					if valuesMap[column] != nil {
						v = i2float(valuesMap[column])
						if column == "cluster_uptime" {
							v = i2float(valuesMap[column]) / 86400
						}
					} else {
						v = nil
					}
				}
				values[i] = append(values[i], &Value{
					Label: label,
					Value: v,
				})
			}
		}

		for i, column := range table.ColumnList {
			metric := p.metricMap[p.buildFQName(tableName, column)]
			for _, value := range values[i] {
				logger.Tracef("set metric:%s, Label:%v, Value:%v", column, value.Label, value.Value)
			}
			if metric.GetValue() != nil {
				values[i] = append(values[i], metric.GetValue()...)
			}
			metric.SetValue(values[i])
		}
	}
}

func (p *Processor) buildFQName(tableName string, column string) string {

	// keep same metric name
	tempFQName := tableName + "_" + column
	if _, ok := metricNameMap[tempFQName]; ok {
		return p.prefix + "_" + metricNameMap[tempFQName]
	}

	b := pool.BytesPoolGet()
	b.WriteString(p.prefix)
	b.WriteByte('_')

	b.WriteString(tableName)

	if column != "" {
		b.WriteByte('_')
		b.WriteString(column)
	}

	fqName := b.String()
	pool.BytesPoolPut(b)

	return fqName
}

func (p *Processor) GetMetric() map[string]*Metric {
	return p.metricMap
}

func (p *Processor) Close() error {
	close(p.exitChan)
	return p.dbConn.Close()
}

func getRoleStr(v float64) string {
	rounded := math.Round(v)
	integer := int(rounded)

	switch integer {
	case 0:
		return "offline"
	case 100:
		return "follower"
	case 101:
		return "candidate"
	case 102:
		return "leader"
	case 103:
		return "error"
	case 104:
		return "learner"
	}
	return "unknown"
}

func getStatusStr(v float64) string {
	rounded := math.Round(v)
	integer := int(rounded)

	switch integer {
	case 0:
		return "offline"
	case 1:
		return "ready"
	}
	return "unknown"
}

func exchangeDBType(t string) CollectType {
	switch t {
	case "BOOL", "FLOAT", "DOUBLE":
		return Gauge
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED":
		return Counter
	case "BINARY", "NCHAR", "VARCHAR":
		return Info
	default:
		panic("unsupported type")
	}
}

func i2string(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		panic("unexpected type to string")
	}
}

func i2float(value interface{}) float64 {
	switch v := value.(type) {
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		panic("unexpected type to float64")
	}
}
