package main

import (
	"runtime"

	_ "taosainternal/admin"

	"github.com/taosdata/taosadapter/system"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	router := system.Init()
	system.Start(router)
}
