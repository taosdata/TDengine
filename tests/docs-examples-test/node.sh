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


pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

sleep 10

cd ../../docs/examples/node

npm install

cd websocketexample

node all_type_query.js

node all_type_stmt.js

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
node line_example.js

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
node nodejsChecker.js

node sql_example.js

node stmt_example.js

node tmq_example.js

node tmq_seek_example.js