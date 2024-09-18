package collect

import (
	"collector/client"
	"collector/client/opcda"
	"collector/client/opcua"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/reporter"
	"collector/watcher"
	"context"
	"crypto/md5"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var logger = log.GetLogger("collect")
var configPath string

var CollectCommand = &cobra.Command{
	Use:   "collect",
	Short: "Collect opc data",
	Long:  "Collect opc data from opc server",
	Run: func(cmd *cobra.Command, args []string) {
		collect()
	},
}

func collect() {
	if configPath == "" {
		logger.Panic("config file is null")
		return
	}
	// create opc client
	bs, err := ioutil.ReadFile(configPath)
	if err != nil {
		logger.Panicf("read config file error. file:%s, err: %v", configPath, err)
		return
	}
	logger.Infof("config file content: %s", bs)
	conf, err := config.ParseConfigBs(bs)
	if err != nil {
		logger.Panic("parse config file error.", "error", err)
		return
	}
	lastMD5 := getMD5(configPath)
	logger.Infof("config: %+v\n", conf)
	err = conf.ValidateCollect()
	if err != nil {
		logger.WithError(err).Panic("validate config file error.")
	}
	if conf.Debug {
		log.SetLevel("debug")
		enablePprof()
	}
	var opcClient client.OPCClient
	wc, err := watcher.NewWatcher(log.GetLogger("watcher"), func(file string) {
		if opcClient != nil {
			newMD5 := getMD5(configPath)
			if lastMD5 == newMD5 {
				return
			}
			logger.Info("config file changed, reload config.")
			lastMD5 = newMD5
			c, err := config.ParseConfig(configPath)
			if err != nil {
				logger.WithError(err).Error("parse config file error.")
				return
			}
			opcClient.ChangeCollectConfig(c.Collect)
		}
	}, configPath)
	if err != nil {
		logger.WithError(err).Panic("new watcher error")
	}
	defer wc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// create report manager
	manager := reporter.NewManager(ctx, conf.Report)
	defer manager.Close()
	switch conf.OpcType {
	case config.OpcTypeUA:
		opcClient, err = opcua.NewUAClient(ctx, conf.Connect.Ua, 0, logger)
	case config.OpcTypeDA:
		opcClient, err = opcda.NewDAClient(ctx, conf.Connect.Da, 0, logger)
	default:
		logger.Panic("not support opc type", "type", conf.OpcType)
	}
	if err != nil {
		logger.WithError(err).Panic("new opc client error")
	}
	defer opcClient.Close()
	// connect opc server
	err = opcClient.Connect()
	if err != nil {
		logger.WithError(err).Panic("connect error")
	}
	err = opcClient.Collect(conf.Collect, handleMessage(manager))
	if err != nil {
		logger.WithError(err).Panic("collect error")
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	logger.Info("receive stop signal, stop collect opc data")
}

func handleMessage(manager *reporter.Manager) client.OnMessage {
	return func(message []*common.NodeValue) {
		if len(message) == 0 {
			return
		}
		var m = map[*reporter.ArrowReporter][]*common.NodeValue{}
		for _, v := range message {
			if v == nil {
				continue
			}
			r, err := manager.GetReporter(v.IDStr, v.ValueType)
			if err != nil {
				logger.WithField("identifier", v.IDStr).WithError(err).Error("get reporter error")
				continue
			}
			m[r] = append(m[r], v)
		}
		for r, v := range m {
			r.Report(v)
		}
	}
}

func enablePprof() {
	listenAddr := ":0"

	server := &http.Server{
		Addr:    listenAddr,
		Handler: http.DefaultServeMux,
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.WithError(err).Panic("enable pprof error")
	}
	addr := ln.Addr()
	logger.Infof("pprof server listening on %s", addr.String())
	server.Close()
	go server.Serve(ln)
}

func getMD5(fileName string) [16]byte {
	tmp, _ := os.ReadFile(fileName)
	return md5.Sum(tmp)
}

func init() {
	CollectCommand.Flags().StringVarP(&configPath, "conf", "c", "", "use --conf to set config path")
	CollectCommand.MarkFlagRequired("conf")
}
