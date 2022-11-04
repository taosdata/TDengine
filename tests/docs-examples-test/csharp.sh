#!/bin/bash

set -e

pgrep taosd || taosd 2>&1 >/dev/null &	
pgrep taosadapter || taosadapter 2>&1 >/dev/null &	
jobs

cd ../../docs/examples/csharp
jobs
dotnet run --project connect/connect.csproj
jobs
taos -s "drop database if exists power"
dotnet run --project sqlInsert/sqlinsert.csproj
dotnet run --project query/query.csproj
dotnet run --project asyncQuery/asyncquery.csproj
dotnet run --project subscribe/subscribe.csproj
jobs
taos -s "drop topic if exists topic_example"
taos -s "drop database if exists power"
dotnet run --project stmtInsert/stmtinsert.csproj
jobs
taos -s "drop database if exists test"
dotnet run --project influxdbLine/influxdbline.csproj
jobs
taos -s "drop database if exists test"
dotnet run --project optsTelnet/optstelnet.csproj
jobs
taos -s "drop database if exists test"
dotnet run --project optsJSON/optsJSON.csproj
jobs
taos -s "create database if not exists test"
jobs
dotnet run --project wsConnect/wsConnect.csproj
dotnet run --project wsInsert/wsInsert.csproj
dotnet run --project wsStmt/wsStmt.csproj
dotnet run --project wsQuery/wsQuery.csproj

taos -s "drop database if exists test"
taos -s "drop database if exists power"