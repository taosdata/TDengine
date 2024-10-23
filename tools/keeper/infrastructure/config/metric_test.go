package config_test

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/version"
	"io"
	"os"
	"runtime"
	"testing"
)

func TestConfig(t *testing.T) {
	data := `
# Start with debug middleware for gin
debug = true
# Listen port, default is 6043
port = 9000
# log level
loglevel = "error"
# go pool size
gopoolsize = 5000
# interval for TDengine metrics
RotationInterval = "10s"
[tdengine]
address = "http://localhost:6041"
authtype = "Basic"
username = "root"
password = "taosdata"
`
	var c config.Config
	_, err := toml.Decode(data, &c)
	if err != nil {
		t.Error(err)
		return
	}
	assert.EqualValues(t, c, c)
	fmt.Print(c)
}
func TestBakConfig(t *testing.T) {
	isOk := copyConfigFile()
	if isOk {
		config.Name = "aaa"
		config.InitConfig()
		config.Name = "taoskeeper"
	}
}

func copyConfigFile() bool {
	var sourceFile string
	var destinationFile string
	switch runtime.GOOS {
	case "windows":
		sourceFile = fmt.Sprintf("C:\\%s\\cfg\\%s.toml", version.CUS_NAME, "taoskeeper")
		destinationFile = fmt.Sprintf("C:\\%s\\cfg\\%s.toml", version.CUS_NAME, "keeper")
	default:
		sourceFile = fmt.Sprintf("/etc/%s/%s.toml", version.CUS_PROMPT, "taoskeeper")
		destinationFile = fmt.Sprintf("/etc/%s/%s.toml", version.CUS_PROMPT, "keeper")
	}
	_, err := os.Stat(sourceFile)
	if os.IsNotExist(err) {
		return false
	}

	source, err := os.Open(sourceFile) //open the source file
	if err != nil {
		panic(err)
	}
	defer source.Close()

	destination, err := os.Create(destinationFile) //create the destination file
	if err != nil {
		panic(err)
	}
	defer destination.Close()
	_, err = io.Copy(destination, source) //copy the contents of source to destination file
	if err != nil {
		panic(err)
	}
	return true
}
