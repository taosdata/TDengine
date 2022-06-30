#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/csharp

dotnet run --project connect.csproj

taos -s "drop database if exists power"
dotnet run --project sqlinsert.csproj
dotnet run --project query.csproj
dotnet run --project asyncquery.csproj

taos -s "drop database if exists power"
dotnet run --project stmtinsert.csproj

taos -s "drop database if exists test"
dotnet run --project influxdbline.csproj

taos -s "drop database if exists test"
dotnet run --project optstelnet.csproj

taos -s "drop database if exists test"
dotnet run --project optsjson.csproj
