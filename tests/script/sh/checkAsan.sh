#!/bin/bash

set +e
#set -x

export LD_PRELOAD=
SCRIPT_DIR=`dirname $0`
cd $SCRIPT_DIR/../
SCRIPT_DIR=`pwd`

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TAOS_DIR=`pwd`
LOG_DIR=$TAOS_DIR/sim/asan

error_num=`cat ${LOG_DIR}/*.asan | grep "ERROR" | wc -l`
memory_leak=`cat ${LOG_DIR}/*.asan | grep "Direct leak" | wc -l`
indirect_leak=`cat ${LOG_DIR}/*.asan | grep "Indirect leak" | wc -l`
runtime_error=`cat ${LOG_DIR}/*.asan | grep "runtime error" | grep -v "trees.c:873" | wc -l`

echo -e "\033[44;32;1m"asan error_num: $error_num"\033[0m"
echo -e "\033[44;32;1m"asan memory_leak: $memory_leak"\033[0m"
echo -e "\033[44;32;1m"asan indirect_leak: $indirect_leak"\033[0m"
echo -e "\033[44;32;1m"asan runtime error: $runtime_error"\033[0m"

let "errors=$error_num+$memory_leak+$indirect_leak+$runtime_error"

if [ $errors -eq 0 ]; then
  echo  -e "\033[44;32;1m"no asan errors"\033[0m"
  exit 0
else
  echo  -e "\033[44;31;1m"asan total errors: $errors"\033[0m"
  cat ${LOG_DIR}/*.asan
  exit 1
fi