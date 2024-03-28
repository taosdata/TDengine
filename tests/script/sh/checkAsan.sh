#!/bin/bash

set +e
#set -x
if [[ "$OSTYPE" == "darwin"* ]]; then
  TD_OS="Darwin"
else
  OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
  len=$(echo ${#OS})
  len=$((len - 2))
  TD_OS=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
fi

if [[ "$TD_OS" == "Alpine" ]]; then
  echo -e "os is Alpine,skip check Asan"
  exit 0
fi
unset LD_PRELOAD
SCRIPT_DIR=$(dirname $0)
cd $SCRIPT_DIR/../
SCRIPT_DIR=$(pwd)

IN_TDINTERNAL="community"
if [[ "$SCRIPT_DIR" == *"$IN_TDINTERNAL"* ]]; then
  cd ../../..
else
  cd ../../
fi

TAOS_DIR=$(pwd)
LOG_DIR=$TAOS_DIR/sim/asan

error_num=$(cat ${LOG_DIR}/*.asan | grep "ERROR" | wc -l)

archOs=$(arch)
if [[ $archOs =~ "aarch64" ]]; then
  echo "arm64 check mem leak"
  memory_leak=$(cat ${LOG_DIR}/*.asan | grep "Direct leak" | grep -v "Direct leak of 32 byte" | wc -l)
  memory_count=$(cat ${LOG_DIR}/*.asan | grep "Direct leak of 32 byte" | wc -l)

  if [ $memory_count -eq $error_num ] && [ $memory_leak -eq 0 ]; then
    echo "reset error_num to 0, ignore: __cxa_thread_atexit_impl leak"
    error_num=0
  fi
else
  echo "os check mem leak"
  memory_leak=$(cat ${LOG_DIR}/*.asan | grep "Direct leak" | wc -l)
fi

indirect_leak=$(cat ${LOG_DIR}/*.asan | grep "Indirect leak" | wc -l)
python_error=$(cat ${LOG_DIR}/*.info | grep -w "stack" | wc -l)

# ignore

# TD-20368
# /root/TDengine/contrib/zlib/trees.c:873:5: runtime error: null pointer passed as argument 2, which is declared to never be null

# TD-20494 TD-20452
# /root/TDengine/source/libs/scalar/src/sclfunc.c:735:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'signed char'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:790:11: runtime error: 3.4e+38 is outside the range of representable values of type 'long int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:772:11: runtime error: 3.52344e+09 is outside the range of representable values of type 'int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:753:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'short int'

# TD-20569
# /root/TDengine/source/libs/function/src/builtinsimpl.c:856:29: runtime error: signed integer overflow: 9223372036854775806 + 9223372036854775805 cannot be represented in type 'long int'
# /root/TDengine/source/libs/scalar/src/sclvector.c:1075:66: runtime error: signed integer overflow: 9223372034707292160 + 1668838476672 cannot be represented in type 'long int'
# /root/TDengine/source/common/src/tdataformat.c:1876:7: runtime error: signed integer overflow: 8252423483843671206 + 2406154664059062870 cannot be represented in type 'long int'
# /home/chr/TDengine/source/libs/scalar/src/filter.c:3149:14: runtime error: applying non-zero offset 18446744073709551615 to null pointer

# /home/chr/TDengine/source/libs/scalar/src/filter.c:3149:14: runtime error: applying non-zero offset 18446744073709551615 to null pointer
# /home/TDinternal/community/source/libs/scalar/src/sclvector.c:1109:66: runtime error: signed integer overflow: 9223372034707292160 + 1676867897049 cannot be represented in type 'long int'

#0 0x7f2d64f5a808 in __interceptor_malloc ../../../../src/libsanitizer/asan/asan_malloc_linux.cc:144
#1 0x7f2d63fcf459 in strerror /build/glibc-SzIz7B/glibc-2.31/string/strerror.c:38
runtime_error=$(cat ${LOG_DIR}/*.asan | grep "runtime error" | grep -v "trees.c:873" | grep -v "sclfunc.c.*outside the range of representable values of type" | grep -v "signed integer overflow" | grep -v "strerror.c" | grep -v "asan_malloc_linux.cc" | grep -v "strerror.c" | grep -v "asan_malloc_linux.cpp" | grep -v "sclvector.c" | wc -l)

echo -e "\033[44;32;1m"asan error_num: $error_num"\033[0m"
echo -e "\033[44;32;1m"asan memory_leak: $memory_leak"\033[0m"
echo -e "\033[44;32;1m"asan indirect_leak: $indirect_leak"\033[0m"
echo -e "\033[44;32;1m"asan runtime error: $runtime_error"\033[0m"
echo -e "\033[44;32;1m"asan python error: $python_error"\033[0m"

let "errors=$error_num+$memory_leak+$indirect_leak+$runtime_error+$python_error"

if [ $errors -eq 0 ]; then
  echo -e "\033[44;32;1m"no asan errors"\033[0m"
  exit 0
else
  echo -e "\033[44;31;1m"asan total errors: $errors"\033[0m"
  if [ $python_error -ne 0 ]; then
    cat ${LOG_DIR}/*.info
  fi
  cat ${LOG_DIR}/*.asan
  exit 1
fi
