#!/bin/bash

set -e

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &
sleep 1
cd ../../docs/examples/go

go mod tidy

go run ./connect/afconn/main.go
go run ./connect/cgoexample/main.go
go run ./connect/restexample/main.go
go run ./connect/connpool/main.go
go run ./connect/wsexample/main.go

taos -s "drop database if exists power"
go run ./sqlquery/main.go

taos -s "drop database if exists power"
sleep 1
go run ./queryreqid/main.go

taos -s "drop database if exists power"
sleep 1
go run ./stmt/native/main.go

taos -s "drop database if exists power"
sleep 1
go run ./stmt/ws/main.go

taos -s "drop database if exists power"
sleep 1
go run ./schemaless/native/main.go

taos -s "drop database if exists power"
sleep 1
go run ./schemaless/ws/main.go

taos -s "drop topic if exists topic_meters"
sleep 1
taos -s "drop database if exists power"
sleep 1
go run ./tmq/native/main.go

taos -s "drop topic if exists topic_meters"
sleep 1
taos -s "drop database if exists power"
sleep 1
go run ./tmq/ws/main.go


taos -s "drop database if exists test"
sleep 1
go run ./insert/json/main.go
taos -s "drop database if exists test"
sleep 1
go run ./insert/line/main.go
taos -s "drop topic if exists topic_meters"
sleep 1
taos -s "drop database if exists power"
sleep 1
go run ./insert/sql/main.go
taos -s "drop database if exists power"
sleep 1
go run ./insert/stmt/main.go
taos -s "drop database if exists test"
sleep 1
go run ./insert/telnet/main.go

go run ./query/sync/main.go

taos -s "drop topic if exists example_tmq_topic"
sleep 1
taos -s "drop database if exists example_tmq"
sleep 1
go run ./sub/main.go
