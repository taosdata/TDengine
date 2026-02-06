package points

import (
	"collector/client"
	"collector/client/opcda"
	"collector/client/opcua"
	"collector/common"
	"collector/config"
	"collector/log"
	"context"
	"encoding/json"
	"os"

	"github.com/spf13/cobra"
)

var logger = log.GetLogger("points")
var configPath string
var PointsCommand = &cobra.Command{
	Use:   "points",
	Short: "Get points",
	Long:  "Get points from opc server",
	Run: func(cmd *cobra.Command, args []string) {
		getAllPoint()
	},
}

func getAllPoint() {
	if configPath == "" {
		logger.Panic("config file is null")
		return
	}

	conf, err := config.ParseConfig(configPath)
	if err != nil {
		logger.Panic("parse config file error.", "error", err)
		return
	}
	err = conf.ValidateGetPoints()
	if err != nil {
		logger.WithError(err).Panic("validate config file error.")
	}
	if conf.Debug {
		log.SetLevel("debug")
	}
	var opcClient client.OPCClient
	ctx := context.Background()
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
	err = opcClient.Connect()
	if err != nil {
		logger.WithError(err).Panic("connect error")
	}
	points, err := opcClient.GetAllPoints(conf.Points)
	if err != nil {
		logger.WithError(err).Panic("get all points error")
	}
	if len(points) == 0 {
		points = []*common.Point{}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(points)
	logger.Debugf("get points success, total: %d", len(points))
}

func init() {
	PointsCommand.Flags().StringVarP(&configPath, "conf", "c", "", "use --conf to set config path")
	PointsCommand.MarkFlagRequired("conf")
}
