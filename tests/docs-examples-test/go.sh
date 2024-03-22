#!/bin/bash

set -e

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &
sleep 10
cd ../../docs/examples/go

go mod tidy

go run ./connect/afconn/main.go
go run ./connect/cgoexample/main.go
go run ./connect/restexample/main.go

taos -s "drop database if exists test"
go run ./insert/json/main.go
taos -s "drop database if exists test"
go run ./insert/line/main.go
taos -s "drop database if exists power"
go run ./insert/sql/main.go
taos -s "drop database if exists power"
go run ./insert/stmt/main.go
taos -s "drop database if exists test"
go run ./insert/telnet/main.go

go run ./query/sync/main.go

taos -s "drop topic if exists example_tmq_topic"
taos -s "drop database if exists example_tmq"
go run ./sub/main.go

taos -s "drop database if exists power"
go run ./demo/query/main.go

taos -s "drop database if exists power"
go run ./demo/sml/main.go

taos -s "drop database if exists power"
go run ./demo/smlws/main.go

taos -s "drop database if exists power"
go run ./demo/stmt/main.go

taos -s "drop database if exists power"
go run ./demo/stmtws/main.go

taos -s "drop TOPIC IF EXISTS topic_meters"
taos -s "drop database if exists power"
go run ./demo/consumer/main.go

sleep 5
taos -s "drop TOPIC IF EXISTS topic_meters"
sleep 5
taos -s "drop database if exists power"
go run ./demo/consumerws/main.go
