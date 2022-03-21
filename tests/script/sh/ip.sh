#!/bin/sh

#already create real card, such as 192.168.0.1-5
#exit 0

if [ $# != 4 ]; then 
  echo "argument list need input : "
  echo "  -i if use [192.168.0.70 ] then input [70]"
  echo "  -s up/down"
  exit 1
fi

#just create ip like 192.168.0.*

IP_ADDRESS=
EXEC_OPTON=
while getopts "i:s:" arg 
do
  case $arg in
    i)
      IP_ADDRESS=$OPTARG
      ;;
    s)
      EXEC_OPTON=$OPTARG
      ;;
    ?)
      echo "unkonw argument"
      ;;
  esac
done

echo ============ $EXEC_OPTON $IP_ADDRESS ===========
sudo ifconfig lo:$IP_ADDRESS 192.168.0.$IP_ADDRESS $EXEC_OPTON
