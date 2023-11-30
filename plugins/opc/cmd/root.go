package cmd

import (
	"collector/cmd/check"
	"collector/cmd/collect"
	"collector/cmd/points"
	versionCmd "collector/cmd/version"
	"collector/version"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "taosx-opc",
	Short: "taosx-opc is an opc connector",
	Long:  "taosx-opc is an opc connector",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stderr, "version: %s\n", version.GetVersion())
		fmt.Fprintf(os.Stderr, "commit id: %s\n", version.GetCommitID())
		fmt.Fprintf(os.Stderr, "build time: %s\n", version.GetBuildDate())
		fmt.Fprintf(os.Stderr, "pid: %d\n", os.Getpid())
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(points.PointsCommand)
	rootCmd.AddCommand(versionCmd.VersionCommand)
	rootCmd.AddCommand(check.CheckCommand)
	rootCmd.AddCommand(collect.CollectCommand)
}
