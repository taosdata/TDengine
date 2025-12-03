#!/bin/bash

##################################################
#
# Do simulation test
#
##################################################

set +e
# set -x

# Detect OS type
if [[ "${OSTYPE}" == "darwin"* ]]; then
  TD_OS="Darwin"
else
  # Extract OS name safely
  OS=$(grep "^NAME=" /etc/*-release | head -n1 | cut -d= -f2 | tr -d '"')
  TD_OS=$(echo "${OS}" | awk '{print $1}')
fi

export ASAN_OPTIONS=detect_odr_violation=0

# Get script directory
SCRIPT_DIR="$(dirname "$0")"
# set test code directory
TEST_CODE_DIR="$(dirname "$SCRIPT_DIR")"

IN_TDINTERNAL="community"
if [[ "${SCRIPT_DIR}" == *"${IN_TDINTERNAL}"* ]]; then
  TOP_DIR="${TEST_CODE_DIR}/../../"
else
  TOP_DIR="${TEST_CODE_DIR}/../"
fi

cd "$TOP_DIR" || exit
TAOSD_DIR=$(find . -name "taosd" | grep bin | head -n1)
cut_opt="-f"
if [[ "${TAOSD_DIR}" == *"${IN_TDINTERNAL}"* ]]; then
  BIN_DIR=$(find . -name "taosd" | grep bin | head -n1 | cut -d '/' ${cut_opt}2,3)
else
  BIN_DIR=$(find . -name "taosd" | grep bin | head -n1 | cut -d '/' ${cut_opt}2)
fi

BUILD_DIR="${TOP_DIR}/${BIN_DIR}"
SIM_DIR="${TOP_DIR}/sim"
PRG_DIR="${SIM_DIR}/tsim"
ASAN_DIR="${SIM_DIR}/asan"

chmod -R 777 "$PRG_DIR"
echo "------------------------------------------------------------------------"
echo "Start TDengine Testing Case ..."
echo "BUILD_DIR: ${BUILD_DIR}"
echo "SIM_DIR  : ${SIM_DIR}"
echo "TEST_CODE_DIR : ${TEST_CODE_DIR}"
echo "ASAN_DIR : ${ASAN_DIR}"

# Prevent deleting / folder or /usr/bin
if [ ${#SIM_DIR} -lt 10 ]; then
  echo "len(SIM_DIR) < 10 , danger so exit. SIM_DIR=${SIM_DIR}"
  exit 1
fi

rm -rf "${SIM_DIR:?}/"*

mkdir -p "${PRG_DIR}"
mkdir -p "${ASAN_DIR}"

cd "${TEST_CODE_DIR}" || exit
ulimit -n 600000
ulimit -c unlimited

# sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e
echo "ExcuteCmd:" "$@"

if [[ "$TD_OS" == "Alpine" ]]; then
  "$@"
else
  AsanFile="${ASAN_DIR}/psim.info"
  echo "AsanFile:" "$AsanFile"

  unset LD_PRELOAD
  # export LD_PRELOAD=libasan.so.5
  # export LD_PRELOAD=$(gcc -print-file-name=libasan.so)
  LD_PRELOAD="$(realpath "$(gcc -print-file-name=libasan.so)") $(realpath "$(gcc -print-file-name=libstdc++.so)")"
  export LD_PRELOAD
  echo "Preload AsanSo:" $?

  "$@" -A 2>"$AsanFile"

  unset LD_PRELOAD
  for ((i = 1; i <= 20; i++)); do
    AsanFileLen=$(wc -l < "${AsanFile}")
    echo "AsanFileLen:" "${AsanFileLen}"
    if [ "$AsanFileLen" -gt 10 ]; then
      break
    fi
    sleep 1
  done

  # Check case successful
  AsanFileSuccessLen=$(grep -w -a -c "successfully executed" "$AsanFile")
  echo "AsanFileSuccessLen:" "$AsanFileSuccessLen"

  if [[ "$AsanFileSuccessLen" -gt 0 ]]; then
    echo "Execute script successfully and check asan"
    # TODO: to be refactored, need to check if taos* process is closed successfully
    sleep 3
    "$TEST_CODE_DIR"/ci/checkAsan.sh
  else
    echo "Execute script failure"
    exit 1
  fi
fi