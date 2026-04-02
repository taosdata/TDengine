package collectd

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers/collectd"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

var logger = log.GetLogger("PLG").WithField("mod", "collectd")

type MetricWithClientIP struct {
	ClientIP net.IP
	Metric   []telegraf.Metric
}
type Plugin struct {
	conf       Config
	conn       *net.UDPConn
	parser     *collectd.Parser
	metricChan chan *MetricWithClientIP
	closeChan  chan struct{}
}

func (p *Plugin) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("collectd disabled")
		return nil
	}
	p.parser = &collectd.Parser{
		ParseMultiValue: "split",
	}
	return p.parser.Init()
}

func (p *Plugin) Start() error {
	if !p.conf.Enable {
		return nil
	}
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			return err
		}
	}
	conn, err := udpListen("udp", fmt.Sprintf(":%d", p.conf.Port))
	if err != nil {
		return err
	}
	p.closeChan = make(chan struct{})
	p.metricChan = make(chan *MetricWithClientIP, 2*p.conf.Worker)
	for i := 0; i < p.conf.Worker; i++ {
		go func() {
			serializer := &influx.Serializer{}
			_ = serializer.Init()
			for {
				select {
				case metric := <-p.metricChan:
					p.HandleMetrics(serializer, metric.ClientIP, metric.Metric)
				case <-p.closeChan:
					return
				}
			}
		}()
	}
	p.conn = conn
	go p.listen()
	return nil
}

func (p *Plugin) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	close(p.closeChan)
	return nil
}

func (p *Plugin) String() string {
	return "collectd"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) HandleMetrics(serializer *influx.Serializer, clientIP net.IP, metrics []telegraf.Metric) {
	if len(metrics) == 0 {
		return
	}

	for _, metric := range metrics {
		if metric.Time().IsZero() {
			metric.SetTime(time.Now())
		}
	}
	data, err := serializer.SerializeBatch(metrics)
	if err != nil {
		logger.Errorf("serialize collectd error, err:%s", err)
		return
	}
	taosConn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, p.conf.Token, clientIP)
	if err != nil {
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			logger.Errorf("whitelist forbidden, user:%s, clientIP:%s", p.conf.User, clientIP.String())
			return
		}
		logger.Errorf("connect server error, err:%s", err)
		return
	}
	defer func() {
		logger.Tracef("put connection")
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	isDebug := log.IsDebug()
	reqID := generator.GetReqID()
	execLogger := logger.WithField(config.ReqIDKey, reqID)
	execLogger.Debugf("insert lines, data:%s, db:%s, ttl:%d", data, p.conf.DB, p.conf.TTL)
	start := log.GetLogNow(isDebug)
	err = inserter.InsertInfluxdb(taosConn.TaosConnection, data, p.conf.DB, "ns", p.conf.TTL, uint64(reqID), "", execLogger)
	logger.Debugf("insert lines finish, cost:%s", log.GetLogDuration(isDebug, start))
	if err != nil {
		logger.Errorf("insert lines error, err:%s, data:%s", err, data)
		return
	}
}

func (p *Plugin) listen() {
	buf := make([]byte, 64*1024) // 64kb - maximum size of IP packet
	for {
		n, addr, err := p.conn.ReadFrom(buf)
		if monitor.AllPaused() {
			continue
		}
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				logger.Error(err.Error())
			}
			break
		}
		if addr == nil {
			logger.Error("addr is nil,ignore data")
			continue
		}
		metrics, err := p.parser.Parse(buf[:n])
		if err != nil {
			logger.Errorf("Unable to parse incoming packet: %s", err.Error())
			continue
		}
		p.metricChan <- &MetricWithClientIP{
			ClientIP: addr.(*net.UDPAddr).IP,
			Metric:   metrics,
		}
	}
}

func udpListen(network string, address string) (*net.UDPConn, error) {
	var addr *net.UDPAddr
	var err error
	addr, err = net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP(network, addr)
}

func init() {
	plugin.Register(&Plugin{})
}
