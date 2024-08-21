#!/bin/bash

set -e

check_transactions() {
    for i in {1..30}
    do
        output=$(taos -s "show transactions;")
        if [[ $output == *"Query OK, 0 row(s)"* ]]; then
            echo "Success: No transactions are in progress."
            return 0
        fi
        sleep 1
    done

    echo "Error: Transactions are still in progress after 30 attempts."
    return 1
}

reset_cache() {
  response=$(curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' --data 'reset query cache')

  if [[ $response == \{\"code\":0* ]]; then
    echo "Success: Query cache reset successfully."
  else
    echo "Error: Failed to reset query cache. Response: $response"
    return 1
  fi
}

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
check_transactions || exit 1
reset_cache || exit 1
go run ./sqlquery/main.go

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./queryreqid/main.go

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./stmt/native/main.go

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./stmt/ws/main.go

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
sleep 3
go run ./schemaless/native/main.go

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./schemaless/ws/main.go

taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./tmq/native/main.go

taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./tmq/ws/main.go


taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
go run ./insert/json/main.go
taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
go run ./insert/line/main.go
taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./insert/sql/main.go
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
go run ./insert/stmt/main.go
taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
go run ./insert/telnet/main.go

go run ./query/sync/main.go

taos -s "drop topic if exists example_tmq_topic"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists example_tmq"
check_transactions || exit 1
reset_cache || exit 1
go run ./sub/main.go
