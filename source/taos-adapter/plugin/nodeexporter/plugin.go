package nodeexporter

import (
	"context"
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
	"unsafe"

	"github.com/gin-gonic/gin"
	tmetric "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

var logger = log.GetLogger("PLG").WithField("mod", "NodeExporter")

type NodeExporter struct {
	conf     Config
	request  []*Req
	exitChan chan struct{}
}
type Req struct {
	req    *http.Request
	client *http.Client
	url    string
}

func (p *NodeExporter) Init(_ gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("node_exporter disabled")
		return nil
	}
	err := p.prepareUrls()
	if err != nil {
		return err
	}
	return nil
}

func (p *NodeExporter) Start() error {
	if !p.conf.Enable {
		return nil
	}
	p.exitChan = make(chan struct{})
	ticker := time.NewTicker(p.conf.GatherDuration)
	go func() {
		for {
			select {
			case <-ticker.C:
				if monitor.AllPaused() {
					continue
				}
				p.Gather()
			case <-p.exitChan:
				ticker.Stop()
				ticker = nil
				return
			}
		}
	}()
	return nil
}

func (p *NodeExporter) Stop() error {
	if !p.conf.Enable {
		return nil
	}
	if p.exitChan != nil {
		close(p.exitChan)
	}
	return nil
}

func (p *NodeExporter) String() string {
	return "node_exporter"
}

func (p *NodeExporter) Version() string {
	return "v1"
}

func (p *NodeExporter) prepareUrls() error {
	authToken := ""
	if len(p.conf.HttpBearerTokenString) != 0 {
		authToken = "Bearer " + p.conf.HttpBearerTokenString
	} else if len(p.conf.HttpUsername) != 0 {
		auth := p.conf.HttpUsername + ":" + p.conf.HttpPassword
		authToken = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	}
	certPool := x509.NewCertPool()
	if len(p.conf.CaCertFile) != 0 {
		caCert, err := os.ReadFile(p.conf.CaCertFile)
		if err != nil {
			return err
		}
		certPool.AppendCertsFromPEM(caCert)
	}
	var certificates []tls.Certificate
	if len(p.conf.CertFile) != 0 {
		cert, err := tls.LoadX509KeyPair(p.conf.CertFile, p.conf.KeyFile)
		if err != nil {
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
		Timeout: p.conf.ResponseTimeout,
	}
	for _, u := range p.conf.URLs {
		URL, err := url.Parse(u)
		if err != nil {
			return err
		}
		var req *http.Request
		if URL.Scheme == "unix" {
			path := URL.Query().Get("path")
			if path == "" {
				path = "/metrics"
			}
			addr := "http://localhost" + path
			req, err = http.NewRequest("GET", addr, nil)
			if err != nil {
				return fmt.Errorf("unable to create new request '%s': %s", addr, err)
			}

			uClient := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig:   tlsCfg,
					DisableKeepAlives: true,
					DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
						c, err := net.Dial("unix", URL.Path)
						return c, err
					},
				},
				Timeout: p.conf.ResponseTimeout,
			}
			if len(authToken) != 0 {
				req.Header.Set("Authorization", authToken)
			}
			p.request = append(p.request, &Req{
				req:    req,
				client: uClient,
				url:    u,
			})
		} else {
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
			p.request = append(p.request, &Req{
				req:    req,
				client: c,
				url:    u,
			})
		}
	}
	return nil
}

var localhost = net.IPv4(127, 0, 0, 1)

func (p *NodeExporter) Gather() {
	conn, err := commonpool.GetConnection(p.conf.User, p.conf.Password, p.conf.Token, localhost)
	if err != nil {
		logger.WithError(err).Errorln("commonpool.GetConnection error")
		return
	}
	defer func() {
		err = conn.Put()
		if err != nil {
			logger.WithError(err).Errorln("conn.Put error")
		}
	}()
	for _, req := range p.request {
		err := p.requestSingle(conn.TaosConnection, req)
		if err != nil {
			logger.WithError(err).Errorln("gather")
		}
	}
}

func (p *NodeExporter) requestSingle(conn unsafe.Pointer, req *Req) error {
	resp, err := req.client.Do(req.req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", req.req.URL, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %s", err)
	}
	metrics, err := Parse(body, resp.Header, p.conf.IgnoreTimestamp)
	if err != nil {
		return fmt.Errorf("error pase body: %s", err)
	}
	serializer := &influx.Serializer{}
	_ = serializer.Init()
	for _, metric := range metrics {
		metric.AddTag("url", req.url)
		tags := metric.Tags()
		m := tmetric.New(metric.Name(), tags, metric.Fields(), metric.Time(), metric.Type())
		data, err := serializer.Serialize(m)
		if err != nil {
			return err
		}
		reqID := generator.GetReqID()
		execLogger := logger.WithField(config.ReqIDKey, reqID)
		err = inserter.InsertInfluxdb(conn, data, p.conf.DB, "ns", p.conf.TTL, uint64(reqID), "", execLogger)
		if err != nil {
			logger.Errorf("insert influxdb error, err: %s, data: %s", err, data)
			return err
		}
	}
	return nil
}

func init() {
	plugin.Register(&NodeExporter{})
}
