package system

import (
	"context"
	"fmt"
	"runtime"
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

var defaultBatchSize = 1024

var defaultWorkerCount = runtime.NumCPU()
var defaultReceiveChanLen = 65535
var defaultBatchTimeout = time.Second

type program struct {
	messageQueue         chan *report.Message
	configFile           string
	workerMessageHandler []*report.MessageList
	dump                 *log.DataDump
	reporters            []*report.ArrowReporter
	logger               *logrus.Entry
	mqttConnector        connector.MQTTConnector
	connected            chan struct{}
	exit                 chan struct{}
	exitFinish           chan struct{}
	once                 sync.Once
}

func newProgram(configFile string) *program {
	return &program{
		configFile: configFile,
		connected:  make(chan struct{}),
		exit:       make(chan struct{}),
		exitFinish: make(chan struct{}),
	}
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
	if conf.Dump != nil {
		if conf.Dump.Enable {
			p.dump, err = log.NewDataDump(conf.Dump.Path, conf.Dump.Keep)
			if err != nil {
				p.logger.WithError(err).Error("new data dump fail")
				return errors.WithMessage(err, "new data dump fail")
			}
		}
	}
	batchSize := defaultBatchSize
	batchTimeout := defaultBatchTimeout
	receiveChanLen := defaultReceiveChanLen
	worker := defaultWorkerCount
	if conf.Batch != nil {
		if conf.Batch.BatchSize > 0 {
			batchSize = conf.Batch.BatchSize
		}
		if conf.Batch.BatchTimeout > 0 {
			batchTimeout = time.Duration(conf.Batch.BatchTimeout) * time.Millisecond
		}
		if conf.Batch.ReceiveChanLen > 0 {
			receiveChanLen = conf.Batch.ReceiveChanLen
		}
		if conf.Batch.Worker > 0 {
			worker = conf.Batch.Worker
		}
	}
	p.logger = log.GetLogger("main")
	if service.Interactive() {
		p.logger.Info("Running in terminal.")
	} else {
		p.logger.Info("Running under service manager.")
	}
	p.messageQueue = make(chan *report.Message, receiveChanLen)
	p.workerMessageHandler = make([]*report.MessageList, worker)
	p.reporters = make([]*report.ArrowReporter, worker)
	for i := 0; i < worker; i++ {
		p.workerMessageHandler[i] = report.NewMessageList(batchSize, i)
		reporter, err := report.NewArrowReporter(conf.Remote, i)
		if err != nil {
			p.logger.WithError(err).Error("new arrow reporter")
			return errors.WithMessage(err, "new arrow reporter")
		}
		p.reporters[i] = reporter
	}
	p.startWorker(worker)
	mqttLogger := log.GetLogger("mqtt_connect").WithField("addr", conf.MQTT.Address)
	if conf.MQTT.Version == "5.0" {
		p.mqttConnector = mqttv5.NewConnector(conf.MQTT, mqttLogger, p.onConnect, p.onDisconnected, p.onMessage)
	} else {
		p.mqttConnector = mqttv3.NewConnector(conf.MQTT, mqttLogger, p.onConnect, p.onDisconnected, p.onMessage)
	}

	for i := 0; i < worker; i++ {
		go p.handleMessage(batchTimeout, i, p.workerMessageHandler[i], p.reporters[i])
	}
	go func() {
		for {
			select {
			case <-p.exit:
				return
			case <-p.connected:
				p.logger.Info("mqtt server connected")
				err = p.mqttConnector.SubscribeMultiple(conf.Topics)
				if err != nil {
					p.logger.WithError(err).WithField("topics", conf.Topics).Fatalf("subscribe fail")
				}
			}
		}
	}()

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
		for _, reporter := range p.reporters {
			if reporter != nil {
				reporter.Close()
			}
		}
		p.logger.Info("server stopped")
	})
	return nil
}

func (p *program) onMessage(qos byte, topic string, payload []byte) {
	logger := p.logger.WithFields(map[string]interface{}{
		"qos":     int(qos),
		"topic":   topic,
		"payload": payload,
	})
	logger.Debugln("got message")
	select {
	case p.messageQueue <- &report.Message{
		TS:      time.Now().UnixMilli(),
		Topic:   topic,
		Qos:     qos,
		Payload: payload,
	}:
		logger.Debugln("write to queue")
	default:
		logger.Warnln("message queue is full,message will be dropped")
	}
}

func (p *program) startWorker(worker int) {
	for i := 0; i < worker; i++ {
		messageManager := p.workerMessageHandler[i]
		go func() {
			for {
				select {
				case <-p.exit:
					return
				case msg := <-p.messageQueue:
					messageManager.Add(msg)
					if p.dump != nil {
						p.dump.Dump(time.UnixMilli(msg.TS), msg.Qos, msg.Topic, msg.Payload)
					}
				}
			}
		}()
	}
}

func (p *program) onConnect() {
	p.connected <- struct{}{}
}

func (p *program) onDisconnected(err error) {
	p.logger.WithError(err).Error("mqtt disconnected")
}

func (p *program) handleMessage(tick time.Duration, id int, messageHandle *report.MessageList, reporter *report.ArrowReporter) {
	ticker := time.NewTicker(tick)
	logger := p.logger.WithField("worker_id", id)
	for {
		select {
		case <-p.exit:
			logger.Debug("handle message received exit signal")
			list := messageHandle.GetAll()
			logger.Debugf("handle message received exit signal get all count %d", len(list))
			if len(list) > 0 {
				err := reporter.Report(list)
				if err != nil {
					logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
				}
			}
			p.exitFinish <- struct{}{}
			ticker.Stop()
			return
		case <-ticker.C:
			logger.Debug("handle message timeout")
			list := messageHandle.GetAll()
			logger.Debugf("handle message timeout get all count %d", len(list))

			if len(list) > 0 {
				err := reporter.Report(list)
				if err != nil {
					logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
				}
			}
		case <-messageHandle.C():
			logger.Info("handle message received signal")
			list := messageHandle.GetAll()
			logger.Debugf("handle message received signal get all count %d", len(list))

			if len(list) > 0 {
				err := reporter.Report(list)
				if err != nil {
					logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
				}
			}
		}
	}
}

func CheckConnection(configFile string) error {
	conf, err := config.ParseConfig(configFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if conf.MQTT.Version == "5.0" {
		err = mqttv5.Check(ctx, conf.MQTT)
	} else {
		err = mqttv3.Check(ctx, conf.MQTT)
	}
	if err != nil {
		return err
	}
	return nil
}
