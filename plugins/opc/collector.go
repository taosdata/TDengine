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
)

func main() {
	log.SetOutput(os.Stderr) // log to stderr

	ctx := context.Background()
	points := flag.NewFlagSet("points", flag.ExitOnError)
	pointConfigPath := points.String("conf", "", "use --conf to set config path")

	coll := flag.NewFlagSet("collect", flag.ExitOnError)
	collectConfigPath := coll.String("conf", "", "use --conf to set config path")

	if len(os.Args) < 2 {
		log.Printf("## param error %v", os.Args[1:])
		os.Exit(1)
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
		log.Printf("## unknown command %s ", os.Args[1])
		os.Exit(1)
	}
}

func getAllNodes(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		log.Println("## config file is null")
		os.Exit(1)
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		log.Println("## parse config file error ", err)
		os.Exit(1)
	}
	pointer, err := worker.NewOpcPointer(config)
	if err != nil {
		log.Println("## new opc pointer error ", err)
		os.Exit(1)
	}
	defer pointer.Exist(ctx)
	points, err := pointer.GetAllPoints(ctx)
	if err != nil {
		log.Println("## get all pointer error ", err)
		os.Exit(1)
	}
	j, _ := json.Marshal(points)
	fmt.Println(string(j))
}

func collect(ctx context.Context, configPath *string) {
	if configPath == nil || len(*configPath) == 0 {
		log.Println("## config file is null")
		os.Exit(1)
	}
	config, err := common.ParseConfig(*configPath)
	if err != nil {
		log.Println("## parse config file error ", err)
		os.Exit(1)
	}
	collector, err := worker.NewCollector(config)
	if err != nil {
		log.Println("## new opc collector error ", err)
		os.Exit(1)
	}
	defer collector.Stop(ctx)
	err = collector.Collect(ctx)
	if err != nil {
		log.Println("## collect opc data error ", err)
		os.Exit(1)
	}
}

func showVersion() {
	fmt.Println(version.Version)
}
