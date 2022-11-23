#!/bin/bash

##################################################
# 
# Do simulation test 
#
##################################################

set -e
#set -x
VALGRIND=0
LOG_BK_DIR=/data/valgrind_log_backup     # 192.168.0.203
SIM_FILES=./jenkins/basic.txt
cases_task_file=../parallel_test/cases.task

cat $cases_task_file | grep "./test.sh " | awk -F, '{print $5}' > $SIM_FILES

while getopts "v:r:f:" arg
do
  case $arg in
    v)
      VALGRIND=1
      ;;
    r)
      LOG_BK_DIR=$(echo $OPTARG)
      ;;
    #f)
    #  SIM_FILES=$(echo $OPTARG)
    #  ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "VALGRIND: $VALGRIND, LOG_BK_DIR: $LOG_BK_DIR"
echo "SIM_FILES: $SIM_FILES"

CURRENT_DIR=`pwd`
TSIM_LOG_DIR=$CURRENT_DIR/../../sim/tsim/log
TAOSD_LOG_DIR=$CURRENT_DIR/../../sim

echo "tsim  log dir: $TSIM_LOG_DIR"
echo "taosd log dir: $TAOSD_LOG_DIR"

if [[ $VALGRIND -eq 1 ]]; then
  if [ -d ${LOG_BK_DIR} ]; then
    rm -rf ${LOG_BK_DIR}/*
  else
    mkdir -p $LOG_BK_DIR/
  fi
fi

while read line
do
  firstChar=`echo ${line:0:1}`
  if [[ -n "$line" ]]  && [[ $firstChar != "#" ]]; then
    if [[ $VALGRIND -eq 1 ]]; then
      echo "======== $line -v ========" 
      $line -v 
      
      # move all valgrind log files of the sim case to valgrind back dir
      # get current sim case name for       
      result=`echo ${line%sim*}`
      result=`echo ${result#*/}`
      result=`echo ${result#*/}`
      result=`echo ${result////-}`
      tsimLogFile=valgrind-${result}sim.log

      echo "cp ${TSIM_LOG_DIR}/valgrind-tsim.log ${LOG_BK_DIR}/${tsimLogFile}    "
      cp ${TSIM_LOG_DIR}/valgrind-tsim.log ${LOG_BK_DIR}/${tsimLogFile}    
      cp ${TAOSD_LOG_DIR}/dnode1/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode2/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode3/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode4/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode5/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode6/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode7/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode8/log/valgrind*.log ${LOG_BK_DIR}/          ||:
      cp ${TAOSD_LOG_DIR}/dnode9/log/valgrind*.log ${LOG_BK_DIR}/          ||:

    else
      echo "======== $line ========"
      $line
    fi
  fi
done < ${SIM_FILES}
#done < ./jenkins/basic.txt


