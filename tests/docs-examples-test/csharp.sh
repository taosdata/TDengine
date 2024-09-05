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
cd ../../docs/examples/csharp

dotnet run --project connect/connect.csproj
dotnet run --project wsConnect/wsConnect.csproj

taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project influxdbLine/influxdbline.csproj

taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project optsTelnet/optstelnet.csproj

taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project optsJSON/optsJSON.csproj

# query
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project wsInsert/wsInsert.csproj
dotnet run --project wsQuery/wsQuery.csproj

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project sqlInsert/sqlinsert.csproj
dotnet run --project query/query.csproj


# stmt
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project wsStmt/wsStmt.csproj

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project stmtInsert/stmtinsert.csproj

# schemaless
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project wssml/wssml.csproj

taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project nativesml/nativesml.csproj

# subscribe
taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project wssubscribe/wssubscribe.csproj

taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
dotnet run --project subscribe/subscribe.csproj