package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
)

func main() {
	dsn := os.Getenv("TDENGINE_GO_DSN")
	taos, err := sql.Open("taosRestful", dsn)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer taos.Close()
	rows, err := taos.Query("select server_version()")
	if err != nil {
		fmt.Println(err)
		return
	}
	rows.Close()
	fmt.Println("connect success")
}
