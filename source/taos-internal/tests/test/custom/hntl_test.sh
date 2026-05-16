#!/bin/sh

# if [ $# != 4 || $# != 5 ]; then 
  # echo "argument list need input : "
  # echo "  -n nodeName"
  # echo "  -s start/stop"
  # echo "  -c clear"
  # exit 1
# fi

THREAD_NUM=6
THREAD_KILL=false
while getopts "t:k" arg 
do
  case $arg in
    t)
      THREAD_NUM=$OPTARG
      ;;    
    k)
      THREAD_KILL=true
      ;; 
	?)
      echo "unkonw argument"
      ;;
  esac
done

if [ "$THREAD_KILL" = "false" ]; then 
  count=0
  while [ $count -lt $THREAD_NUM ]; do
    BEGIN_TABLE=$((count*1000000+1))
#	echo nohup ./hntl_test -m 0 -e mt -t t -q 16 -f 1484228800 -p "ts timestamp,  Ua int, Ub int, Uc int, Ia int, Ib int, Ic int, P int, Q int, p9 int, p10 int, p11 int, p12 int, p13 int, p14 int, p15 int, p16 int, p17 int, p18 int, p19 int, p20 int" -a 40000000 -b 16 -r 20 -d db -i 0 -g $BEGIN_TABLE 
    nohup ./hntl_test -n 1 -m 0 -e mt -t t -q 16 -f 1485228800 -p "ts timestamp,  Ua int, Ub int, Uc int, Ia int, Ib int, Ic int, P int, Q int, p9 int, p10 int, p11 int, p12 int, p13 int, p14 int, p15 int, p16 int, p17 int, p18 int, p19 int, p20 int" -a 1000000 -b 16 -r 5 -d db -i 0 -g $BEGIN_TABLE  > /dev/null 2>&1 & 
    count=$((count + 1))
  done

  #nohup $EXE_DIR/taosd -c $CFG_DIR > /dev/null 2>&1 & 
else
  PID=`ps -ef|grep hntl_test | grep -v grep | awk '{print $2}'`
  if [ -n "$PID" ]; then 
    sudo kill -9 $PID
  fi 
fi

