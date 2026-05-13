package version

import (
	"collector/version"
	"fmt"

	"github.com/spf13/cobra"
)

var VersionCommand = &cobra.Command{
	Use:   "version",
	Short: "Print the version",
	Long:  "Print the version and exit",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version.ShowVersion())
	},
}
