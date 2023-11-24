package collect

import (
	"collector/client"
	"collector/client/opcda"
	"collector/client/opcua"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/reporter"
	"context"
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
	conf, err := config.ParseConfig(configPath)
	if err != nil {
		logger.Panic("parse config file error.", "error", err)
		return
	}
	err = conf.ValidateCollect()
	if err != nil {
		logger.WithError(err).Panic("validate config file error.")
	}
	if conf.Debug {
		log.SetLevel("debug")
	}

	var opcClient client.OPCClient
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// create report manager
	manager := reporter.NewManager(ctx, conf.Report)
	defer manager.Close()
	switch conf.OpcType {
	case config.OpcTypeUA:
		opcClient, err = opcua.NewUAClient(ctx, conf.Connect.Ua, conf.Collect, 0, logger, handleMessage(manager))
	case config.OpcTypeDA:
		opcClient, err = opcda.NewDAClient(ctx, conf.Connect.Da, conf.Collect, 0, logger, handleMessage(manager))
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
	err = opcClient.Collect()
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
			r, err := manager.GetReporter(v.Identifier, v.ValueType)
			if err != nil {
				logger.WithError(err).Error("get reporter error")
				continue
			}
			m[r] = append(m[r], v)
		}
		for r, v := range m {
			r.Report(v)
		}
	}
}

func init() {
	CollectCommand.Flags().StringVarP(&configPath, "conf", "c", "", "use --conf to set config path")
	CollectCommand.MarkFlagRequired("conf")
}
