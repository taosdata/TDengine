package main

import (
	"github.com/taosdata/taoskeeper/system"
)

func main() {
	r := system.Init()
	system.Start(r)
	// config.IsEnterprise
}
