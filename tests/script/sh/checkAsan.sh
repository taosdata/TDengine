#!/bin/bash

set +e
#set -x

unset LD_PRELOAD
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

# ignore
# /root/TDengine/source/libs/scalar/src/sclfunc.c:735:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'signed char'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:790:11: runtime error: 3.4e+38 is outside the range of representable values of type 'long int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:772:11: runtime error: 3.52344e+09 is outside the range of representable values of type 'int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:753:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'short int'
runtime_error=`cat ${LOG_DIR}/*.asan | grep "runtime error" | grep -v "trees.c:873" | grep -v "sclfunc.c.*outside the range of representable values of type" | wc -l`

python_error=`cat ${LOG_DIR}/*.info | grep -w "stack" | wc -l`

echo -e "\033[44;32;1m"asan error_num: $error_num"\033[0m"
echo -e "\033[44;32;1m"asan memory_leak: $memory_leak"\033[0m"
echo -e "\033[44;32;1m"asan indirect_leak: $indirect_leak"\033[0m"
echo -e "\033[44;32;1m"asan runtime error: $runtime_error"\033[0m"
echo -e "\033[44;32;1m"asan python error: $python_error"\033[0m"

let "errors=$error_num+$memory_leak+$indirect_leak+$runtime_error+$python_error"

if [ $errors -eq 0 ]; then
  echo  -e "\033[44;32;1m"no asan errors"\033[0m"
  exit 0
else
  echo  -e "\033[44;31;1m"asan total errors: $errors"\033[0m"
  if [ $python_error -ne 0 ]; then
    cat ${LOG_DIR}/*.info
  fi
  cat ${LOG_DIR}/*.asan
  exit 1
fi