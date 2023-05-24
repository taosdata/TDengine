package mqttv5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosx/plugins/mqtt/config"
	"github.com/taosdata/taosx/plugins/mqtt/connector"
)

type Connector struct {
	logger         logrus.FieldLogger
	client         *autopaho.ConnectionManager
	conf           *config.MQTT
	onConnect      func()
	onDisconnected func(err error)
	onMessage      connector.OnMessage
	cancel         context.CancelFunc
}

func (conn *Connector) SubscribeMultiple(topics map[string]int) error {
	Subscriptions := make(map[string]paho.SubscribeOptions, len(topics))
	for topic, qos := range topics {
		Subscriptions[topic] = paho.SubscribeOptions{
			QoS: byte(qos),
		}
	}
	if _, err := conn.client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: Subscriptions,
	}); err != nil {
		return fmt.Errorf("failed to subscribe (%s). This is likely to mean no messages will be received", err)
	}
	return nil
}

func (conn *Connector) Publish(topic string, qos byte, retained bool, payload []byte) error {
	_, err := conn.client.Publish(context.Background(), &paho.Publish{
		QoS:     qos,
		Retain:  retained,
		Topic:   topic,
		Payload: payload,
	})
	return err
}

func (conn *Connector) Stop() {
	if conn.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		conn.client.Disconnect(ctx)
	}
}

func (conn *Connector) connect(conf *config.MQTT) {
	addr, err := url.Parse(conf.Address)
	if err != nil {
		conn.logger.WithError(err).Fatal("MQTT address error")
	}
	schema := strings.ToLower(addr.Scheme)
	cliCfg := autopaho.ClientConfig{
		BrokerUrls: []*url.URL{addr},
		KeepAlive:  uint16(conf.KeepAlive),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			conn.onConnect()
		},
		OnConnectError: func(err error) {
			conn.logger.WithError(err).Fatal("error whilst attempting connection")
		},
		ClientConfig: paho.ClientConfig{
			ClientID: conf.ClientID,
			Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
				conn.onMessage(m.QoS, m.Topic, m.Payload)
			}),
			OnClientError: func(err error) { fmt.Printf("server requested disconnect: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					conn.onDisconnected(fmt.Errorf("server requested disconnect: %s\n", d.Properties.ReasonString))
					fmt.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					conn.onDisconnected(fmt.Errorf("server requested disconnect; reason code: %d\n", d.ReasonCode))
				}
			},
		},
	}
	if schema == "ssl" || schema == "wss" {
		tlsConfig, err := newTLSConfig(conf)
		if err != nil {
			conn.logger.WithError(err).Fatal("wrong tls info")
		}
		cliCfg.TlsCfg = tlsConfig
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn.cancel = cancel
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		conn.logger.WithError(err).Fatal("connect error")
	}
	conn.client = cm
}

func newTLSConfig(conf *config.MQTT) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	caCert := []byte(conf.CA)
	certPool.AppendCertsFromPEM(caCert)

	cert, err := tls.X509KeyPair([]byte(conf.Cert), []byte(conf.CertKey))
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		RootCAs:            certPool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}, nil
}

func NewConnector(conf *config.MQTT, logger logrus.FieldLogger, onConnect connector.OnConnect, onDisconnected connector.OnDisconnected, onMessage connector.OnMessage) *Connector {
	conn := &Connector{logger: logger, conf: conf, onConnect: onConnect, onDisconnected: onDisconnected, onMessage: onMessage}
	conn.connect(conf)
	return conn
}
