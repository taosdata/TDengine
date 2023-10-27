package main

import (
	"collector/common"
	"collector/connector/opcda"
	"collector/connector/opcua"
	"collector/version"
	"collector/worker"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/sunpe/gobox/logger"
)

func main() {
	logger.Init(logger.WithWriter(os.Stderr))
	signal.Ignore(syscall.SIGPIPE) // ignore SIGPIPE

	logger.InfoF("## version: %s", version.GetVersion())
	logger.InfoF("## commit id: %s", version.GetCommitID())
	logger.InfoF("## build time: %s", version.GetBuildDate())
	logger.InfoF("## pid: %d", os.Getpid())
	ctx := context.Background()
	points := flag.NewFlagSet("points", flag.ExitOnError)
	pointConfigPath := points.String("conf", "", "use --conf to set config path")

	coll := flag.NewFlagSet("collect", flag.ExitOnError)
	collectConfigPath := coll.String("conf", "", "use --conf to set config path")

	check := flag.NewFlagSet("check", flag.ExitOnError)
	checkConfigPath := check.String("conf", "", "use --conf to set config path")

	if len(os.Args) < 2 {
		logger.Panic("## param error.", "params", os.Args[1:])
		return
	}

	switch os.Args[1] {
	case "points":
		_ = points.Parse(os.Args[2:])
		getAllNodes(ctx, pointConfigPath)
	case "collect":
		_ = coll.Parse(os.Args[2:])
		collect(ctx, collectConfigPath)
	case "version":
		showVersion()
	case "check":
		_ = check.Parse(os.Args[2:])
		resp := checkConnection(checkConfigPath)
		b, _ := json.Marshal(resp)
		fmt.Println(string(b))
		return
	default:
		logger.Panic("## unknown command", "command", os.Args[1])
		return
	}
}

func getAllNodes(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		log.Println("## config file is null")
		logger.Panic("## config file is null")
		return
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		logger.Panic("## parse config file error.", "error", err)
		return
	}
	if config.Debug {
		logger.Init(logger.WithWriter(os.Stderr), logger.WithLevel(logger.LevelDebug), logger.WithAttr("pid", os.Getpid()))
	}
	pointer, err := worker.NewOpcPointer(config)
	if err != nil {
		logger.Panic("## new opc pointer error. ", "error", err)
		return
	}
	defer pointer.Exist(ctx)
	points, err := pointer.GetAllPoints(ctx)
	if err != nil {
		logger.Panic("## get all pointer error ", "error", err)
		return
	}
	j, _ := json.Marshal(points)
	fmt.Println(string(j))
}

func collect(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		logger.Panic("## config file is null")
		return
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		logger.Panic("## parse config file error ", "error", err)
		return
	}
	if config.Debug {
		logger.Init(logger.WithWriter(os.Stderr), logger.WithLevel(logger.LevelDebug), logger.WithAttr("pid", os.Getpid()))
	}
	collector, err := worker.NewCollector(ctx, config)
	if err != nil {
		logger.Panic("## new opc collector error ", "error", err)
		return
	}
	defer collector.Stop(ctx)
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		if _, ok := <-ch; ok {
			logger.Warn("## receive stop signal, stop collect opc data")
			collector.Stop(ctx)
			return
		}
	}()
	err = collector.Collect(ctx)
	if err != nil {
		logger.Panic("## collect opc data error ", "error", err)
		return
	}
}

func showVersion() {
	fmt.Println(version.ShowVersion())
}

func checkConnection(configPath *string) CheckResp {
	if configPath == nil || len(*configPath) == 0 {
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    "config file is null",
		}
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    err.Error(),
		}
	}
	switch config.OpcType {
	case common.OpcTypeUA:
		err := opcua.CheckConnection(config)
		if err != nil {
			return CheckResp{
				Valid:      false,
				Support:    true,
				DataSource: "opc",
				Message:    err.Error(),
			}
		}
	case common.OpcTypeDA:
		if runtime.GOOS != "windows" {
			return CheckResp{
				Valid:      false,
				Support:    false,
				DataSource: "opc",
				Message:    "opc da only support windows",
			}
		}
		err := opcda.CheckConnection(config)
		if err != nil {
			return CheckResp{
				Valid:      false,
				Support:    true,
				DataSource: "opc",
				Message:    err.Error(),
			}
		}
	default:
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    fmt.Sprintf("unknown opc type %s", config.OpcType),
		}
	}
	return CheckResp{
		Valid:      true,
		Support:    true,
		DataSource: "opc",
	}
}

type CheckResp struct {
	Valid      bool        `json:"valid"`
	Support    interface{} `json:"support"`
	DataSource string      `json:"data_source"`
	Message    string      `json:"message,omitempty"`
}
