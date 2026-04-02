package main

import "github.com/taosdata/taoskeeper/system"

func main() {
	server := system.Init()
	system.Start(server)
}
