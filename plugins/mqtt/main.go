package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/taosdata/taosx/plugins/mqtt/system"
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
	s := system.NewService(*configFile)
	system.Start(s)
}
