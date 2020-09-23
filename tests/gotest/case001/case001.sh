#!/bin/bash

##################################################
# 
# Do go test 
#
##################################################

set +e
#set -x

script_dir="$(dirname $(readlink -f $0))"
#echo "pwd: $script_dir, para0: $0"

execName=$0
execName=`echo ${execName##*/}`
goName=`echo ${execName%.*}`

###### step 1: start one taosd
scriptDir=$script_dir/../../script/sh
bash $scriptDir/stop_dnodes.sh
bash $scriptDir/deploy.sh -n dnode1 -i 1
bash $scriptDir/cfg.sh -n dnode1 -c walLevel -v 0
bash $scriptDir/exec.sh -n dnode1 -s start

###### step 2: set config item
TAOS_CFG=/etc/taos/taos.cfg
HOSTNAME=`hostname -f`

if [ ! -f ${TAOS_CFG} ]; then
  touch -f $TAOS_CFG
fi 

echo " "                                           > $TAOS_CFG
echo "firstEp            ${HOSTNAME}:7100"        >> $TAOS_CFG
echo "secondEp           ${HOSTNAME}:7200"        >> $TAOS_CFG
echo "serverPort         7100"                    >> $TAOS_CFG
#echo "dataDir            $DATA_DIR"              >> $TAOS_CFG
#echo "logDir             $LOG_DIR"               >> $TAOS_CFG
#echo "scriptDir          ${CODE_DIR}/../script"  >> $TAOS_CFG
echo "numOfLogLines      100000000"               >> $TAOS_CFG
echo "dDebugFlag         135"                     >> $TAOS_CFG
echo "mDebugFlag         135"                     >> $TAOS_CFG
echo "sdbDebugFlag       135"                     >> $TAOS_CFG
echo "rpcDebugFlag       135"                     >> $TAOS_CFG
echo "tmrDebugFlag       131"                     >> $TAOS_CFG
echo "cDebugFlag         135"                     >> $TAOS_CFG
echo "httpDebugFlag      135"                     >> $TAOS_CFG
echo "monitorDebugFlag   135"                     >> $TAOS_CFG
echo "udebugFlag         135"                     >> $TAOS_CFG
echo "tablemetakeeptimer 5"                       >> $TAOS_CFG
echo "wal                0"                       >> $TAOS_CFG
echo "asyncLog           0"                       >> $TAOS_CFG
echo "locale             en_US.UTF-8"             >> $TAOS_CFG
echo "enableCoreFile     1"                       >> $TAOS_CFG
echo " "                                          >> $TAOS_CFG  

ulimit -n 600000
ulimit -c unlimited
#
##sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e
#

###### step 3: start build
cd $script_dir
rm -f go.*
go mod init $goName
go build 
sleep  1s
sudo ./$goName
