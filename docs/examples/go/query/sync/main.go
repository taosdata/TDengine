package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
	var taosDSN = "root:taosdata@http(localhost:6041)/power"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("failed to connect TDengine, err:", err)
	}
	defer taos.Close()
	rows, err := taos.Query("SELECT ts, current FROM meters LIMIT 2")
	if err != nil {
		log.Fatalln("failed to select from table, err:", err)
	}

	defer rows.Close()
	for rows.Next() {
		var r struct {
			ts      time.Time
			current float32
		}
		err := rows.Scan(&r.ts, &r.current)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		log.Println(r.ts, r.current)
	}
}
