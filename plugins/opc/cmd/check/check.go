package check

import (
	"collector/client"
	"collector/client/opcda"
	"collector/client/opcua"
	"collector/config"
	"collector/log"
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

var logger = log.GetLogger("check")
var configPath string

type CheckResp struct {
	Valid      bool     `json:"valid"`
	Support    bool     `json:"support"`
	DataSource string   `json:"data_source"`
	Message    string   `json:"message,omitempty"`
	Namespaces []string `json:"namespaces,omitempty"`
}

var CheckCommand = &cobra.Command{
	Use:   "check",
	Short: "Check connection",
	Long:  "Check connection to opc server",
	Run: func(cmd *cobra.Command, args []string) {
		resp := check()
		b, _ := json.Marshal(resp)
		fmt.Println(string(b))
	},
}

func check() CheckResp {
	if configPath == "" {
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    "config file is null",
		}
	}
	conf, err := config.ParseConfig(configPath)
	if err != nil {
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    err.Error(),
		}
	}
	err = conf.ValidateConnect()
	if err != nil {
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    err.Error(),
		}
	}
	ctx := context.Background()
	var opcClient client.OPCClient
	switch conf.OpcType {
	case config.OpcTypeUA:
		opcClient, err = opcua.NewUAClient(ctx, conf.Connect.Ua, 0, logger)
	case config.OpcTypeDA:
		opcClient, err = opcda.NewDAClient(ctx, conf.Connect.Da, 0, logger)
	default:
		return CheckResp{
			Valid:      false,
			Support:    false,
			DataSource: "opc",
			Message:    fmt.Sprintf("unknown opc type %s", conf.OpcType),
		}
	}
	if err != nil {
		return CheckResp{
			Valid:      false,
			Support:    true,
			DataSource: "opc",
			Message:    err.Error(),
		}
	}
	defer opcClient.Close()
	err = opcClient.Connect()
	if err != nil {
		return CheckResp{
			Valid:      false,
			Support:    true,
			DataSource: "opc",
			Message:    err.Error(),
		}
	}
	if conf.OpcType == config.OpcTypeUA {
		return CheckResp{
			Valid:      true,
			Support:    true,
			DataSource: "opc",
			Namespaces: opcClient.(*opcua.UAClient).Namespaces(),
		}
	}
	return CheckResp{
		Valid:      true,
		Support:    true,
		DataSource: "opc",
	}
}

func init() {
	CheckCommand.Flags().StringVarP(&configPath, "conf", "c", "", "use --conf to set config path")
	CheckCommand.MarkFlagRequired("conf")
}
