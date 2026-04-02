package main

import (
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// Multi-endpoint DSN with auto-reconnect for mid-stream failover.
	baseDSN := "root:taosdata@ws(127.0.0.1:6042,127.0.0.1:6041)/"
	dbName := "example_failover_schemaless"
	setupDSN := baseDSN + "?autoReconnect=true"
	workerDSN := baseDSN + dbName + "?autoReconnect=true"

	// Create database via DSN without db name, then reopen with db in DSN.
	// Schemaless insert depends on connection default database.
	setupClient, err := unified.Open(setupDSN)
	if err != nil {
		log.Fatalln("open setup unified client failed:", err)
	}
	mustExec(setupClient, "create database if not exists "+dbName)
	setupClient.Close()

	client, err := unified.Open(workerDSN)
	if err != nil {
		log.Fatalln("open worker unified client failed:", err)
	}
	defer client.Close()

	line := fmt.Sprintf(
		"meters,location=beijing current=%di32,voltage=%di32 %d",
		10,
		220,
		time.Now().UTC().UnixNano()/int64(time.Millisecond),
	)
	if err = client.SchemalessInsert(common.GetReqID(), line, unified.InfluxDBLineProtocol, "ms", 0, ""); err != nil {
		log.Fatalln("schemaless insert failed:", err)
	}

	rows, err := client.Query(0, fmt.Sprintf("select _ts,current,voltage,location from %s.meters order by _ts desc limit 1", dbName))
	if err != nil {
		log.Fatalln("query failed:", err)
	}
	defer rows.Close()

	values := make([]driver.Value, 4)
	err = rows.Next(values)
	if err != nil {
		if err == io.EOF {
			log.Fatalln("no rows returned")
		}
		log.Fatalln("next failed:", err)
	}
	fmt.Printf("ts=%v current=%v voltage=%v location=%v\n", values[0], values[1], values[2], values[3])

	fmt.Println("failover schemaless example done")
}

func mustExec(client *unified.Client, sqlText string) {
	if _, err := client.Exec(0, sqlText); err != nil {
		log.Fatalf("exec failed, sql=%s, err=%v\n", sqlText, err)
	}
}
