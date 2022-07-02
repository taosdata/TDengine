#!/bin/bash
function stopTaosd {
  echo "Stop taosd"
  sudo systemctl stop taosd || echo 'no sudo or systemctl or stop fail'
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x taosd
    sleep 1
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  done
}
stopTaosd
rm -rf /var/lib/taos/*
rm -rf /var/log/taos/*
nohup taosd -c /etc/taos/ > /dev/null 2>&1 &
sleep 10

# define fun to check if execute correct.
check(){
if [ $1 -eq 0 ]
then
    echo "===================$2 succeed==================="
else
    echo "===================$2 failed==================="
    exit 1
fi
}

cd ../../
WKC=`pwd`
echo "WKC:${WKC}"

# run example with neuget package
cd ${WKC}/tests/examples/C#

dotnet run --project C#checker/C#checker.csproj
check $? C#checker.csproj

dotnet run --project TDengineTest/TDengineTest.csproj
check $? TDengineTest.csproj

dotnet run --project schemaless/schemaless.csproj
check $? schemaless.csproj

dotnet run --project jsonTag/jsonTag.csproj
check $? jsonTag.csproj

dotnet run --project stmt/stmt.csproj
check $? stmt.csproj

dotnet run --project insertCn/insertCn.csproj
check $? insertCn.csproj

cd ${WKC}/tests/examples/C#/taosdemo
dotnet build -c Release
tree | true
./bin/Release/net5.0/taosdemo -c /etc/taos -y
check $? taosdemo
