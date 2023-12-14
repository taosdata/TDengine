#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/csharp

dotnet run --project connect/connect.csproj

taos -s "drop database if exists power"
dotnet run --project sqlInsert/sqlinsert.csproj
dotnet run --project query/query.csproj
#dotnet run --project subscribe/subscribe.csproj

#taos -s "drop topic if exists topic_example"
taos -s "drop database if exists power"
dotnet run --project stmtInsert/stmtinsert.csproj

taos -s "drop database if exists test"
dotnet run --project influxdbLine/influxdbline.csproj

taos -s "drop database if exists test"
dotnet run --project optsTelnet/optstelnet.csproj

taos -s "drop database if exists test"
dotnet run --project optsJSON/optsJSON.csproj

taos -s "create database if not exists test"
taos -s "drop database if exists power"
dotnet run --project wsConnect/wsConnect.csproj
dotnet run --project wsInsert/wsInsert.csproj
dotnet run --project wsQuery/wsQuery.csproj
taos -s "drop database if exists power"
dotnet run --project wsStmt/wsStmt.csproj

taos -s "drop database if exists test"
taos -s "drop database if exists power"
