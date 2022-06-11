package main

import (
	"fmt"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func main() {
	conn, err := wrapper.TaosConnect("localhost", "root", "taosdata", "", 6030)
	defer wrapper.TaosClose(conn)
	if err != nil {
		fmt.Println("fail to connect, err:", err)
	} else {
		fmt.Println("connected")
	}
}
