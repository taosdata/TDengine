package main

import (
	"fmt"

	"github.com/taosdata/driver-go/v2/af"
)

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	defer conn.Close()
	if err != nil {
		fmt.Println("failed to connect, err:", err)
	} else {
		fmt.Println("connected")
	}
}
