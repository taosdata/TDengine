package statsd

import (
	"fmt"
	"net"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/telegraflog"
)

var logger = log.GetLogger("PLG").WithField("mod", "statsd")

type Plugin struct {
	conf       Config
	ac         telegraf.Accumulator
	input      *Statsd
	closeChan  chan struct{}
	metricChan chan telegraf.Metric
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("statsd disabled")
		return nil
	}
	return nil
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.closeChan = make(chan struct{})
	p.metricChan = make(chan telegraf.Metric, 2*p.conf.Worker)
	for i := 0; i < p.conf.Worker; i++ {
		go func() {
			serializer := &influx.Serializer{}
			_ = serializer.Init()
			for {
				select {
				case metric := <-p.metricChan:
					p.HandleMetrics(serializer, metric)
				case <-p.closeChan:
					return
				}
			}
		}()
	}
	p.input = &Statsd{
		User:                   p.conf.User,
		Password:               p.conf.Password,
		Token:                  p.conf.Token,
		Protocol:               p.conf.Protocol,
		ServiceAddress:         fmt.Sprintf(":%d", p.conf.Port),
		MaxTCPConnections:      p.conf.MaxTCPConnections,
		TCPKeepAlive:           p.conf.TCPKeepAlive,
		AllowedPendingMessages: p.conf.AllowedPendingMessages,
		DeleteCounters:         p.conf.DeleteCounters,
		DeleteGauges:           p.conf.DeleteGauges,
		DeleteSets:             p.conf.DeleteSets,
		DeleteTimings:          p.conf.DeleteTimings,
		Log:                    logger,
	}
	p.ac = NewAccumulator(&MetricMakerImpl{logger: telegraflog.NewWrapperLogger(logger)}, p.metricChan)
	err := p.input.Start(p.ac)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(p.conf.GatherInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if monitor.AllPaused() {
					return
				}
				err := p.input.Gather(p.ac)
				if err != nil {
					logger.WithError(err).Error("gather error")
				}
			case <-p.closeChan:
				ticker.Stop()
				ticker = nil
				return
			}
		}

	}()
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	p.input.Stop()
	close(p.closeChan)
	return nil
}

func (p *Plugin) String() string {
	return "statsd"
}

func (p *Plugin) Version() string {
	return "v1"
}

var localhost = net.IPv4(127, 0, 0, 1)

func (p *Plugin) HandleMetrics(serializer *influx.Serializer, metric telegraf.Metric) {
	data, err := serializer.Serialize(metric)
	if err != nil {
		logger.WithError(err).Error("serialize statsd error")
		return
	}
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, p.conf.Token, localhost)
	if err != nil {
		logger.WithError(err).Errorln("connect server error")
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	isDebug := log.IsDebug()
	start := log.GetLogNow(isDebug)
	reqID := generator.GetReqID()
	execLogger := logger.WithField(config.ReqIDKey, reqID)
	execLogger.Debugf("insert line,req_id:0x%x,data: %s", reqID, string(data))
	err = inserter.InsertInfluxdb(taosConn.TaosConnection, data, p.conf.DB, "ns", p.conf.TTL, uint64(reqID), "", execLogger)
	execLogger.Debugf("insert line finish cost:%s", log.GetLogDuration(isDebug, start))
	if err != nil {
		execLogger.WithError(err).Errorln("insert lines error", string(data))
		return
	}
}

type MetricMakerImpl struct {
	logger *telegraflog.WrapperLogger
}

func (m *MetricMakerImpl) LogName() string {
	return "metric"
}

func (m *MetricMakerImpl) MakeMetric(metric telegraf.Metric) telegraf.Metric {
	return metric
}

func (m *MetricMakerImpl) Log() telegraf.Logger {
	return m.logger
}

func init() {
	plugin.Register(&Plugin{})
}
