package main

import (
	"fmt"
	"log"

	"github.com/taosdata/driver-go/v3/af"
)

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	defer conn.Close()
	if err != nil {
		log.Fatalln("failed to connect, err:", err)
	} else {
		fmt.Println("connected")
	}
}
