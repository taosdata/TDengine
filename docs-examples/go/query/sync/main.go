package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
)

func main() {
	var taosDSN = "root:taosdata@http(localhost:6041)/power"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		fmt.Println("failed to connect TDengine, err:", err)
		return
	}
	defer taos.Close()
	rows, err := taos.Query("SELECT ts, current FROM meters LIMIT 2")
	if err != nil {
		fmt.Println("failed to select from table, err:", err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var r struct {
			ts      time.Time
			current float32
		}
		err := rows.Scan(&r.ts, &r.current)
		if err != nil {
			fmt.Println("scan error:\n", err)
			return
		}
		fmt.Println(r.ts, r.current)
	}
}
