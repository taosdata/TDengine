#!/bin/bash

set -e
cd ../../docs-examples/go

go run ./connect/restexample/main.go

taos -s "drop database if exists power"
go run ./insert/sql/main.go 

taos -s "drop database if exists power"
go run ./insert/stmt/main.go

taos -s "drop database if exists test"
go run ./insert/line/main.go

taos -s "drop database if exists test"
go run ./insert/telnet/main.go

taos -s "drop database if exists test"
go run ./insert/json/main.go

go run ./query/sync/main.go