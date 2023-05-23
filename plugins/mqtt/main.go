package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/taosdata/taosx/plugins/mqtt/config"
	"github.com/taosdata/taosx/plugins/mqtt/connector"
	"github.com/taosdata/taosx/plugins/mqtt/log"
	"github.com/taosdata/taosx/plugins/mqtt/report"
	"github.com/taosdata/taosx/plugins/mqtt/version"
)

func main() {
	configFile := flag.String("c", "", "config file")
	v := flag.Bool("version", false, "Print the version and exit")
	flag.Parse()
	if v != nil && *v {
		fmt.Printf("mqtt_plugin: %s\ncommit: %s\nbuild_time: %s\n", version.Version, version.Commit, version.BuildTime)
		os.Exit(0)
	}
	fmt.Printf("mqtt_plugin: %s\ncommit: %s\nbuild_time: %s\n", version.Version, version.Commit, version.BuildTime)
	conf, err := config.ParseConfig(*configFile)
	if err != nil {
		panic(err)
	}
	err = log.SetLevel(conf.LogLevel)
	if err != nil {
		panic(err)
	}
	messages := report.NewMessageList()
	logger := log.GetLogger("main")
	mqttLogger := log.GetLogger("mqtt_connect").WithField("addr", conf.MQTT.Address)
	reporter, err := report.NewArrowReporter(conf.Remote)
	if err != nil {
		logger.WithError(err).Fatal("new arrow reporter")
	}
	connected := make(chan struct{})
	mqttConnector := connector.NewConnector(conf.MQTT, mqttLogger, func() {
		connected <- struct{}{}
	}, func(err error) {
		logger.WithError(err).Fatal("mqtt disconnected")
	}, func(client mqtt.Client, message mqtt.Message) {
		logger.WithField("message", message).Debugln("got message")
		m := message
		messages.Add(&report.Message{
			TS:      time.Now().UnixMilli(),
			Message: m,
		})
	})
	<-connected
	logger.Info("mqtt server connected")
	err = mqttConnector.SubscribeMultiple(conf.Topics)
	if err != nil {
		logger.WithError(err).WithField("topics", conf.Topics).Fatal("subscribe fail")
	}
	exit := make(chan struct{})
	exitFinish := make(chan struct{})
	go func() {
		for {
			select {
			case <-exit:
				list := messages.GetAll()
				if len(list) > 0 {
					err = reporter.Report(list)
					if err != nil {
						logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
					}
				}
				exitFinish <- struct{}{}
				return
			default:
				list := messages.GetAll()
				if len(list) > 0 {
					err = reporter.Report(list)
					if err != nil {
						logger.WithError(err).WithField("list", list).Fatal("report data to taosX error")
					}
				}
			}
		}
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-quit
	mqttConnector.Stop()
	close(exit)
	exitTimeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	select {
	case <-exitTimeout.Done():
		break
	case <-exitFinish:
		break
	}
	logger.Println("stop server")
	reporter.Close()
}
