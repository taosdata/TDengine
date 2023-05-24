package system

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kardianos/service"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosx/plugins/mqtt/config"
	"github.com/taosdata/taosx/plugins/mqtt/connector"
	"github.com/taosdata/taosx/plugins/mqtt/connector/mqttv3"
	"github.com/taosdata/taosx/plugins/mqtt/connector/mqttv5"
	"github.com/taosdata/taosx/plugins/mqtt/log"
	"github.com/taosdata/taosx/plugins/mqtt/report"
)

var programLogger = log.GetLogger("program")

func NewService(configFile string) service.Service {
	prg := newProgram(configFile)
	svcConfig := &service.Config{
		Name:        "taosmqtt",
		DisplayName: "taosmqtt",
		Description: fmt.Sprintf("taosmqtt is an mqtt collector that reports data to taosx"),
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		programLogger.Fatal(err)
	}
	return s
}

func Start(s service.Service) {
	defer func() {
		if e := recover(); e != nil {
			programLogger.Error(e)
		}
	}()
	err := s.Run()
	if err != nil {
		s.Stop()
		programLogger.Fatal(err)
	}
}

type program struct {
	configFile    string
	messages      *report.MessageList
	reporter      *report.ArrowReporter
	logger        *logrus.Entry
	mqttConnector connector.MQTTConnector
	connected     chan struct{}
	exit          chan struct{}
	exitFinish    chan struct{}
	once          sync.Once
}

func newProgram(configFile string) *program {
	return &program{configFile: configFile, connected: make(chan struct{}), exit: make(chan struct{}), exitFinish: make(chan struct{})}
}

func (p *program) Start(s service.Service) error {
	conf, err := config.ParseConfig(p.configFile)
	if err != nil {
		return err
	}
	err = log.SetLevel(conf.LogLevel)
	if err != nil {
		return err
	}
	p.messages = report.NewMessageList()
	p.logger = log.GetLogger("main")
	if service.Interactive() {
		p.logger.Info("Running in terminal.")
	} else {
		p.logger.Info("Running under service manager.")
	}
	mqttLogger := log.GetLogger("mqtt_connect").WithField("addr", conf.MQTT.Address)
	p.reporter, err = report.NewArrowReporter(conf.Remote)
	if err != nil {
		p.logger.WithError(err).Error("new arrow reporter")
		return errors.WithMessage(err, "new arrow reporter")
	}
	if conf.MQTT.Version == "5.0" {
		p.mqttConnector = mqttv5.NewConnector(conf.MQTT, mqttLogger, p.onConnect, p.onDisconnected, p.onMessage)
	} else {
		p.mqttConnector = mqttv3.NewConnector(conf.MQTT, mqttLogger, p.onConnect, p.onDisconnected, p.onMessage)
	}
	<-p.connected
	p.logger.Info("mqtt server connected")
	err = p.mqttConnector.SubscribeMultiple(conf.Topics)
	if err != nil {
		p.logger.WithError(err).WithField("topics", conf.Topics).Error("subscribe fail")
		return errors.WithMessage(err, "subscribe fail")
	}
	go p.handleMessage()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.once.Do(func() {
		p.logger.Info("stop server")
		if p.mqttConnector != nil {
			p.mqttConnector.Stop()
		}
		close(p.exit)
		exitTimeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		select {
		case <-exitTimeout.Done():
			break
		case <-p.exitFinish:
			break
		}
		if p.reporter != nil {
			p.reporter.Close()
		}
		p.logger.Info("server stopped")
	})
	return nil
}

func (p *program) onMessage(qos byte, topic string, payload []byte) {
	p.logger.WithFields(map[string]interface{}{
		"qos":     int(qos),
		"topic":   topic,
		"payload": payload,
	}).Debugln("got message")
	p.messages.Add(&report.Message{
		TS:      time.Now().UnixMilli(),
		Topic:   topic,
		Qos:     qos,
		Payload: payload,
	})
}

func (p *program) onConnect() {
	p.connected <- struct{}{}
}

func (p *program) onDisconnected(err error) {
	p.logger.WithError(err).Fatal("new arrow reporter")
}

func (p *program) handleMessage() {
	for {
		select {
		case <-p.exit:
			list := p.messages.GetAll()
			if len(list) > 0 {
				err := p.reporter.Report(list)
				if err != nil {
					p.logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
				}
			}
			p.exitFinish <- struct{}{}
			return
		default:
			list := p.messages.GetAll()
			if len(list) > 0 {
				err := p.reporter.Report(list)
				if err != nil {
					p.logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
				}
			}
		}
	}
}
