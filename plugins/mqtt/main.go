package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/taosdata/taosx/plugins/mqtt/system"
	"github.com/taosdata/taosx/plugins/mqtt/version"
)

func main() {
	var configFile *string
	var check *bool
	var v *bool
	configFile = flag.String("c", "", "config file")
	v = flag.Bool("version", false, "Print the version and exit")
	check = flag.Bool("check", false, "Check connection to mqtt server and exit")
	flag.Parse()
	if v != nil && *v {
		fmt.Fprintf(os.Stderr, "mqtt_plugin: %s\ncommit: %s\nbuild_time: %s\n", version.Version, version.Commit, version.BuildTime)
		return
	}
	if check != nil && *check {
		err := system.CheckConnection(*configFile)
		if err != nil {
			b, _ := json.Marshal(CheckResp{
				Valid:      false,
				Support:    true,
				DataSource: "mqtt",
				Message:    err.Error(),
			})
			fmt.Println(string(b))
		} else {
			b, _ := json.Marshal(CheckResp{
				Valid:      true,
				Support:    true,
				DataSource: "mqtt",
			})
			fmt.Println(string(b))
		}
		return
	}
	fmt.Fprintf(os.Stderr, "mqtt_plugin: %s\ncommit: %s\nbuild_time: %s\n", version.Version, version.Commit, version.BuildTime)
	s := system.NewService(*configFile)
	system.Start(s)
}

type CheckResp struct {
	Valid      bool        `json:"valid"`
	Support    interface{} `json:"support"`
	DataSource string      `json:"data_source"`
	Message    string      `json:"message,omitempty"`
}
