package mqttv3

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"

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
		if conn.onConnect != nil {
			conn.onConnect()
		}
	}

	opts.OnConnectionLost = func(c mqtt.Client, e error) {
		conn.logger.WithError(e).Error("mqtt connection lost")
		conn.onDisconnected(e)
	}
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
