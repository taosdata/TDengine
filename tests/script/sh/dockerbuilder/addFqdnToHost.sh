#!/bin/bash
#
# deploy test cluster

set -e
#set -x

dnodeNumber=1
subnet="172.33.0.0/16"

while getopts "hn:s:" arg
do
  case $arg in
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    s)
      subnet=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [ dnode number] "
      echo "                -s [ subnet] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

addFqdnToHosts() {
  index=$1
  ipPrefix=$2
  let ipIndex=index+1
  echo "${ipPrefix}.${ipIndex}  node${i}"  >> /etc/hosts
}

ipPrefix=${subnet%.*}
for ((i=1; i<=${dnodeNumber}; i++)); do
  addFqdnToHosts ${i} ${ipPrefix}
done




