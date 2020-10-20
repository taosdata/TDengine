#!/bin/bash

echo "==== start Go connector test cases test ===="

severIp=$1
serverPort=$2

if [ ! -n "$severIp" ]; then
  severIp=127.0.0.1
fi

if [ ! -n "$serverPort" ]; then
  serverPort=6030
fi

bash ./case001/case001.sh $severIp $serverPort
#bash ./case002/case002.sh $severIp $serverPort
#bash ./case003/case003.sh $severIp $serverPort
