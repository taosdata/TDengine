#!/bin/bash
#
# deploy test cluster

set -e
#set -x

dnodeNumber=1

while getopts "hn:" arg
do
  case $arg in
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [ dnode number] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

serverPort=6030

for ((i=2; i<=${dnodeNumber}; i++)); do
    taos -s "create dnode node${i} port ${serverPort};" ||:
    echo "create dnode node${i} port ${serverPort};"
done




