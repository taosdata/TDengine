#!/bin/bash

NNODES=$1
NODE_DIR=$2
NODE_PREFIX=$3
PORT=$4

tport=${PORT}

for (( nodeid=1; nodeid<=${NNODES}; nodeid++ ))
do
  rootDir="${NODE_DIR}/${NODE_PREFIX}${nodeid}"
#   echo "$rootDir"
  mkdir -p ${rootDir}/cfg ${rootDir}/data ${rootDir}/log

  echo "serverPort $(hostname):${PORT}" > ${rootDir}/cfg/taos.cfg
  echo "first   $(hostname):${tport}" >> ${rootDir}/cfg/taos.cfg
  echo "dataDir ${rootDir}/data" >> ${rootDir}/cfg/taos.cfg
  echo "logDir ${rootDir}/log" >> ${rootDir}/cfg/taos.cfg
  echo "numOfMPeers 1" >> ${rootDir}/cfg/taos.cfg
  echo "charset UTF-8" >> ${rootDir}/cfg/taos.cfg
  tport=$((tport+100))
done
