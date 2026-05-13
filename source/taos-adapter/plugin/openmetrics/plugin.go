package openmetrics

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/influxdata/telegraf"
	tmetric "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers/openmetrics"
	"github.com/influxdata/telegraf/plugins/parsers/prometheus"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/telegraflog"
)

var logger = log.GetLogger("PLG").WithField("mod", "OpenMetrics")

type OpenMetrics struct {
	conf     Config
	missions []*Mission
	exitChan chan struct{}
}

type Mission struct {
	req    *http.Request
	client *http.Client
	// open metrics 1.0.0
	openMetricsParser *openmetrics.Parser
	// prometheus 0.0.4
	prometheusParser *prometheus.Parser
	url              string
	db               string
	ttl              int
	reqID            uint64
	logger           *logrus.Entry
	telegrafLogger   telegraf.Logger
}

func (p *OpenMetrics) Init(_ gin.IRouter) error {
	p.conf.setValue(viper.GetViper())
	if !p.conf.Enable {
		logger.Debug("open metrics disabled")
		return nil
	}
	err := p.conf.CheckConfig()
	if err != nil {
		return err
	}
	err = p.prepareUrls()
	if err != nil {
		return err
	}
	return nil
}

func (p *OpenMetrics) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.exitChan = make(chan struct{})
	for i := 0; i < len(p.missions); i++ {
		ticker := time.NewTicker(time.Duration(p.conf.GatherDurationSeconds[i]) * time.Second)
		mission := p.missions[i]
		go func() {
			for {
				select {
				case <-ticker.C:
					if monitor.AllPaused() {
						continue
					}
					err := p.requestSingle(mission)
					if err != nil {
						mission.logger.Errorf("request single error: %s", err)
					}
				case <-p.exitChan:
					ticker.Stop()
					ticker = nil
					return
				}
			}
		}()
	}
	return nil
}

func (p *OpenMetrics) prepareUrls() error {
	for i, u := range p.conf.URLs {
		authToken := ""
		if len(p.conf.HttpBearerTokenStrings) != 0 && len(p.conf.HttpBearerTokenStrings[i]) != 0 {
			authToken = "Bearer " + p.conf.HttpBearerTokenStrings[i]
		} else if len(p.conf.HttpUsernames) != 0 && len(p.conf.HttpUsernames[i]) != 0 {
			auth := p.conf.HttpUsernames[i] + ":" + p.conf.HttpPasswords[i]
			authToken = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		}
		certPool := x509.NewCertPool()
		if len(p.conf.CaCertFiles) != 0 && len(p.conf.CaCertFiles[i]) != 0 {
			caCert, err := os.ReadFile(p.conf.CaCertFiles[i])
			if err != nil {
				logger.Errorf("failed to read ca cert file: %s", err)
				return err
			}
			certPool.AppendCertsFromPEM(caCert)
		}
		var certificates []tls.Certificate
		if len(p.conf.CertFiles) != 0 && len(p.conf.CertFiles[i]) != 0 {
			cert, err := tls.LoadX509KeyPair(p.conf.CertFiles[i], p.conf.KeyFiles[i])
			if err != nil {
				logger.Errorf("failed to load cert and key files: %s", err)
				return err
			}
			certificates = append(certificates, cert)
		}

		tlsCfg := &tls.Config{
			RootCAs:            certPool,
			ClientAuth:         tls.NoClientCert,
			ClientCAs:          nil,
			InsecureSkipVerify: p.conf.InsecureSkipVerify,
			Certificates:       certificates,
		}
		c := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   tlsCfg,
				DisableKeepAlives: true,
			},
			Timeout: time.Duration(p.conf.ResponseTimeoutSeconds[i]) * time.Second,
		}
		URL, err := url.Parse(u)
		if err != nil {
			return err
		}
		var req *http.Request
		if URL.Path == "" {
			URL.Path = "/metrics"
		}
		req, err = http.NewRequest(http.MethodGet, URL.String(), nil)
		if err != nil {
			return fmt.Errorf("unable to create new request '%s': %s", URL.String(), err)
		}

		if len(authToken) != 0 {
			req.Header.Set("Authorization", authToken)
		}
		reqID := generator.GetReqID()
		missionLogger := logger.WithFields(logrus.Fields{
			"url":           u,
			config.ReqIDKey: reqID,
		})
		telegrafLogger := telegraflog.NewWrapperLogger(missionLogger)
		openMetricsParser := &openmetrics.Parser{
			MetricVersion:   1,
			IgnoreTimestamp: p.conf.IgnoreTimestamp,
			Log:             telegrafLogger,
		}
		prometheusParser := &prometheus.Parser{
			MetricVersion:   1,
			IgnoreTimestamp: p.conf.IgnoreTimestamp,
			Log:             telegrafLogger,
		}
		mission := &Mission{
			req:               req,
			client:            c,
			url:               u,
			db:                p.conf.DBs[i],
			logger:            missionLogger,
			openMetricsParser: openMetricsParser,
			prometheusParser:  prometheusParser,
			telegrafLogger:    telegrafLogger,
		}
		if len(p.conf.TTL) != 0 {
			mission.ttl = p.conf.TTL[i]
		}
		p.missions = append(p.missions, mission)
	}
	return nil
}

var localhost = net.IPv4(127, 0, 0, 1)

func (p *OpenMetrics) requestSingle(mission *Mission) error {
	mission.logger.Debug("start request")
	resp, err := mission.client.Do(mission.req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	logger.Debugf("response status code: %d", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", mission.req.URL, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %s", err)
	}
	logger.Debugf("response body: %s", body)
	mission.logger.Debug("finished request")
	var metrics []telegraf.Metric
	parser := "openMetrics"
	if openmetrics.AcceptsContent(resp.Header) {
		mission.openMetricsParser.Header = resp.Header
		metrics, err = mission.openMetricsParser.Parse(body)
	} else {
		mission.prometheusParser.Header = resp.Header
		metrics, err = mission.prometheusParser.Parse(body)
		parser = "prometheus"
	}
	if err != nil {
		return fmt.Errorf("%s parser parse body error: %s, body: %v", parser, err, body)
	}
	if len(metrics) == 0 {
		contentType := resp.Header.Get("Content-Type")
		return fmt.Errorf("no metrics found, contentType:%s, body: %v", contentType, body)
	}
	serializer := &influx.Serializer{}
	_ = serializer.Init()
	buffer := &bytes.Buffer{}
	for _, metric := range metrics {
		metric.AddTag("url", mission.url)
		tags := metric.Tags()
		m := tmetric.New(metric.Name(), tags, metric.Fields(), metric.Time(), metric.Type())
		data, err := serializer.Serialize(m)
		if err != nil {
			return err
		}
		buffer.Write(data)
	}
	mission.logger.Trace("start get connection from pool")
	conn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, p.conf.Token, localhost)
	if err != nil {
		mission.logger.Errorf("commonpool.GetConnection error, err:%s", err)
		return err
	}
	defer func() {
		err = conn.Put()
		if err != nil {
			mission.logger.Errorf("conn.Put error, err:%s", err)
		}
	}()
	mission.logger.Trace("finish get connection from pool")
	err = inserter.InsertInfluxdb(conn.TaosConnection, buffer.Bytes(), mission.db, "ns", mission.ttl, mission.reqID, "", mission.logger)
	if err != nil {
		mission.logger.Errorf("insert OpenMetrics error, err:%s, data: %s", err, buffer.String())
		return err
	}
	mission.logger.Trace("finish insert OpenMetrics")
	return nil
}

func (p *OpenMetrics) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.exitChan != nil {
		close(p.exitChan)
	}
	return nil
}

func (p *OpenMetrics) String() string {
	return "openmetrics"
}

func (p *OpenMetrics) Version() string {
	return "v1"
}

func init() {
	plugin.Register(&OpenMetrics{})
}
