#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/csharp

dotnet run --project connect/connect.csproj
dotnet run --project wsConnect/wsConnect.csproj

taos -s "drop database if exists test"
sleep 1
dotnet run --project influxdbLine/influxdbline.csproj

taos -s "drop database if exists test"
sleep 1
dotnet run --project optsTelnet/optstelnet.csproj

taos -s "drop database if exists test"
sleep 1
dotnet run --project optsJSON/optsJSON.csproj

# query
taos -s "drop database if exists power"
sleep 1
dotnet run --project wsInsert/wsInsert.csproj
dotnet run --project wsQuery/wsQuery.csproj

taos -s "drop database if exists power"
sleep 1
dotnet run --project sqlInsert/sqlinsert.csproj
dotnet run --project query/query.csproj


# stmt
taos -s "drop database if exists power"
sleep 1
dotnet run --project wsStmt/wsStmt.csproj

taos -s "drop database if exists power"
sleep 1
dotnet run --project stmtInsert/stmtinsert.csproj

# schemaless
taos -s "drop database if exists power"
sleep 1
dotnet run --project wssml/wssml.csproj

taos -s "drop database if exists power"
sleep 1
dotnet run --project nativesml/nativesml.csproj

# subscribe
taos -s "drop topic if exists topic_meters"
sleep 1
taos -s "drop database if exists power"
sleep 1
dotnet run --project wssubscribe/wssubscribe.csproj

taos -s "drop topic if exists topic_meters"
sleep 1
taos -s "drop database if exists power"
sleep 1
dotnet run --project subscribe/subscribe.csproj
