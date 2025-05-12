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

sleep 5

cd ../../docs/examples/rust/nativeexample

cargo run --example connect

cargo run --example createdb

cargo run --example insert

cargo run --example query

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1

cargo run --example schemaless

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1


cargo run --example stmt

cargo run --example tmq



cd ../restexample

cargo run --example connect

cargo run --example createdb

cargo run --example insert

cargo run --example query

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
taos -s "create database if not exists power"
cargo run --example schemaless

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1


cargo run --example stmt

cargo run --example tmq
