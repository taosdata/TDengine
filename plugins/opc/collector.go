package main

import (
	"collector/common"
	"collector/version"
	"collector/worker"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.SetOutput(os.Stderr) // log to stderr

	log.Printf("## opc collector version [%s] start ...", version.Version)
	ctx := context.Background()
	points := flag.NewFlagSet("points", flag.ExitOnError)
	pointConfigPath := points.String("conf", "", "use --conf to set config path")

	coll := flag.NewFlagSet("collect", flag.ExitOnError)
	collectConfigPath := coll.String("conf", "", "use --conf to set config path")

	if len(os.Args) < 2 {
		log.Panicf("## param error %v", os.Args[1:])
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
	default:
		log.Panicf("## unknown command %s ", os.Args[1])
		return
	}
}

func getAllNodes(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		log.Panicf("## config file is null")
		return
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		log.Panic("## parse config file error ", err)
		return
	}
	pointer, err := worker.NewOpcPointer(config)
	if err != nil {
		log.Panic("## new opc pointer error ", err)
		return
	}
	defer pointer.Exist(ctx)
	points, err := pointer.GetAllPoints(ctx)
	if err != nil {
		log.Panic("## get all pointer error ", err)
		return
	}
	j, _ := json.Marshal(points)
	fmt.Println(string(j))
}

func collect(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		log.Panic("## config file is null")
		return
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		log.Panic("## parse config file error ", err)
		return
	}
	collector, err := worker.NewCollector(ctx, config)
	if err != nil {
		log.Panic("## new opc collector error ", err)
		return
	}
	defer collector.Stop(ctx)
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		if _, ok := <-ch; ok {
			log.Println("## receive stop signal, stop collect opc data")
			collector.Stop(ctx)
			return
		}
	}()
	err = collector.Collect(ctx)
	if err != nil {
		log.Panic("## collect opc data error ", err)
		return
	}
}

func showVersion() {
	fmt.Println(version.Version)
}
