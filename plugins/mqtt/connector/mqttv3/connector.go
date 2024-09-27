package mqttv3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosx/plugins/mqtt/config"
	"github.com/taosdata/taosx/plugins/mqtt/connector"
)

type Connector struct {
	logger         logrus.FieldLogger
	client         mqtt.Client
	conf           *config.MQTT
	onConnect      connector.OnConnect
	onDisconnected connector.OnDisconnected
	onMessage      connector.OnMessage
}

func newTLSConfig(conf *config.MQTT) (*tls.Config, error) {
	certPool := x509.NewCertPool()
	caCert := []byte(conf.CA)
	certPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		ClientAuth:         tls.NoClientCert,
		InsecureSkipVerify: true,
	}
	if conf.Cert != "" || conf.CertKey != "" {
		cert, err := tls.X509KeyPair([]byte(conf.Cert), []byte(conf.CertKey))
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientAuth = tls.RequestClientCert
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func (conn *Connector) SubscribeMultiple(topics map[string]int) error {
	filter := make(map[string]byte, len(topics))
	for topic, qos := range topics {
		filter[topic] = byte(qos)
	}
	return conn.client.SubscribeMultiple(filter, func(client mqtt.Client, message mqtt.Message) {
		conn.onMessage(message.Qos(), message.Topic(), message.Payload())
	}).Error()
}

func (conn *Connector) connect(conf *config.MQTT) {
	//mqtt.DEBUG = conn.logger
	//mqtt.ERROR = conn.logger
	//mqtt.WARN = conn.logger
	//mqtt.CRITICAL = conn.logger
	if conn.client != nil && conn.client.IsConnected() {
		return
	}
	conn.logger.Info("connect to MQTT Server...")
	opts := mqtt.NewClientOptions()
	opts.ClientID = conf.ClientID
	if opts.ClientID == "" {
		opts.ClientID = strings.ReplaceAll(uuid.New().String(), "-", "")
	}
	opts.Username = conf.Username
	opts.Password = conf.Password
	opts.CleanSession = conf.CleanSession
	opts.KeepAlive = conf.KeepAlive
	if strings.HasPrefix(conf.Address, "ssl") || strings.HasPrefix(conf.Address, "wss") {
		tlsConfig, err := newTLSConfig(conf)
		if err != nil {
			conn.logger.WithError(err).Fatal("wrong tls info")
		}
		opts.TLSConfig = tlsConfig
	}
	opts.AddBroker(conf.Address)
	opts.OnConnect = func(c mqtt.Client) {
		conn.logger.Info("mqtt connected")
		if conn.onConnect != nil {
			conn.onConnect()
		}
	}

	opts.OnConnectionLost = func(c mqtt.Client, e error) {
		conn.logger.WithError(e).Error("mqtt connection lost")
		conn.onDisconnected(e)
	}
	opts.AutoReconnect = true
	opts.MaxReconnectInterval = time.Second * 5

	client := mqtt.NewClient(opts)
	conn.client = client

	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		conn.logger.WithError(token.Error()).Fatal("could not connect to mqtt broker")
	}
}

func (conn *Connector) Publish(topic string, qos byte, retained bool, payload []byte) error {
	if conn.client == nil || !conn.client.IsConnectionOpen() {
		return errors.New("mqtt server not connected")
	}
	return conn.client.Publish(topic, qos, retained, payload).Error()
}

func (conn *Connector) Stop() {
	if conn.client != nil {
		conn.client.Disconnect(1000)
	}
}

func NewConnector(config *config.MQTT, logger logrus.FieldLogger, onConnect connector.OnConnect, onDisconnected connector.OnDisconnected, onMessage connector.OnMessage) *Connector {
	conn := &Connector{
		conf:           config,
		onMessage:      onMessage,
		onConnect:      onConnect,
		onDisconnected: onDisconnected,
		logger:         logger,
	}
	go conn.connect(config)
	return conn
}

func Check(ctx context.Context, conf *config.MQTT) error {
	opts := mqtt.NewClientOptions()
	opts.ClientID = strings.ReplaceAll(uuid.New().String(), "-", "")
	opts.Username = conf.Username
	opts.Password = conf.Password
	opts.CleanSession = true
	opts.ConnectTimeout = time.Second * 5

	if strings.HasPrefix(conf.Address, "ssl") || strings.HasPrefix(conf.Address, "wss") {
		tlsConfig, err := newTLSConfig(conf)
		if err != nil {
			return err
		}
		opts.TLSConfig = tlsConfig
	}
	opts.AddBroker(conf.Address)
	connected := make(chan struct{}, 1)
	connectError := make(chan error, 1)
	opts.OnConnect = func(c mqtt.Client) {
		connected <- struct{}{}
	}

	client := mqtt.NewClient(opts)

	token := client.Connect()
	go func() {
		if token.Wait() {
			err := token.Error()
			if err != nil {
				connectError <- token.Error()
			}
		}
	}()
	select {
	case <-connected:
		return nil
	case err := <-connectError:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
