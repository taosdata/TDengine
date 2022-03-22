package main

import (
	"database/sql"
	"fmt"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
)

func main() {
	var taosDSN = "root:taosdata@http(localhost:6041)/test"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		fmt.Println("failed to connect TDengine, err:", err)
		return
	}
	fmt.Println("Connected")
	defer taos.Close()
}
