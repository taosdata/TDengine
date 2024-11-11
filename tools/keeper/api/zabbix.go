package api

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taoskeeper/process"
	"github.com/taosdata/taoskeeper/util/pool"
)

type Zabbix struct {
	processor  *process.Processor
	floatGroup []*process.Metric
	strGroup   []*process.Metric
}

func NewZabbix(processor *process.Processor) *Zabbix {
	z := &Zabbix{processor: processor}
	z.processorMetrics()
	return z
}

type zabbixMetric struct {
	Data []*ZMetric `json:"data"`
}

type ZMetric struct {
	Metric string      `json:"{#METRIC}"`
	Key    string      `json:"key"`
	Value  interface{} `json:"value"`
}

const (
	FloatType = iota + 1
	StringType
)

func (z *Zabbix) Init(c gin.IRouter) {
	api := c.Group("zabbix")
	api.GET("float", z.getFloat)
	api.GET("string", z.getString)
}

func (z *Zabbix) getFloat(c *gin.Context) {
	z.returnData(c, FloatType)
}

func (z *Zabbix) getString(c *gin.Context) {
	z.returnData(c, StringType)
}

func (z *Zabbix) returnData(c *gin.Context, valueType int) {
	var metrics []*process.Metric
	switch valueType {
	case FloatType:
		metrics = z.floatGroup
	case StringType:
		metrics = z.strGroup
	}
	var d zabbixMetric
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	for _, metric := range metrics {
		values := metric.GetValue()
		for _, value := range values {
			label := z.sortLabel(value.Label)
			b.Reset()
			b.WriteString(metric.FQName)
			if len(label) > 0 {
				b.WriteByte(',')
				b.WriteString(label)
			}
			metricName := b.String()
			d.Data = append(d.Data, &ZMetric{
				Metric: metricName,
				Key:    metricName,
				Value:  value.Value,
			})
		}
	}
	c.JSON(http.StatusOK, d)
}

func (z *Zabbix) sortLabel(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	result := make([]string, 0, len(labels))
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	for k, v := range labels {
		b.Reset()
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
		result = append(result, b.String())
	}
	sort.Strings(result)
	return strings.Join(result, "_")
}

func (z *Zabbix) processorMetrics() {
	metrics := z.processor.GetMetric()
	for _, metric := range metrics {
		if metric.Type == process.Gauge || metric.Type == process.Counter {
			z.floatGroup = append(z.floatGroup, metric)
		} else if metric.Type == process.Info {
			z.strGroup = append(z.strGroup, metric)
		}
	}
}
