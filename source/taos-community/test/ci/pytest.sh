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

# malloc_context_size=10: 每次分配只记录10层调用栈（默认30），大幅减少 ASAN 内存开销，避免 OOM-kill
# quarantine_size_mb=64:  减小 ASAN 隔离区（默认256MB），进一步降低内存峰值
# detect_leaks 默认开启（不设置 detect_leaks=0），由 checkAsan.sh 过滤 Python/numpy 等已知噪声，
#              只统计调用栈中含 TDinternal/TDengine 路径的泄漏帧。
export ASAN_OPTIONS=detect_odr_violation=0:malloc_context_size=10:quarantine_size_mb=64

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# set test code directory
TEST_CODE_DIR="$(dirname "$SCRIPT_DIR")"

IN_TDINTERNAL="community"
if [[ "${SCRIPT_DIR}" == *"taos-community"* ]]; then
  TOP_DIR="${TEST_CODE_DIR}/../../../"
elif [[ "${SCRIPT_DIR}" == *"${IN_TDINTERNAL}"* ]]; then
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

declare -x BUILD_DIR=$TOP_DIR/$BIN_DIR

# Derive SIM_DIR from taosd binary path: place 'sim' alongside 'debug' directory
if [ -n "$TAOSD_DIR" ]; then
  TAOSD_REALPATH=$(realpath "$TOP_DIR/$TAOSD_DIR" 2>/dev/null)
  if [ -n "$TAOSD_REALPATH" ]; then
    _CHECK_PATH=$(dirname "$TAOSD_REALPATH")
    while [ "$_CHECK_PATH" != "/" ]; do
      if [ "$(basename $_CHECK_PATH)" = "debug" ]; then
        declare -x SIM_DIR=$(dirname $_CHECK_PATH)/sim
        break
      fi
      _CHECK_PATH=$(dirname "$_CHECK_PATH")
    done
  fi
fi
if [ -z "$SIM_DIR" ]; then
  declare -x SIM_DIR=$TOP_DIR/sim
fi

PROGRAM=$BUILD_DIR/build/bin/tsim
PRG_DIR=$SIM_DIR/tsim
ASAN_DIR=$SIM_DIR/asan

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
  if [[ "${CI_ASAN_BUILD}" == "1" ]]; then
    # ASAN 构建（others:latest GCC14, libasan.so.8 与测试容器匹配）
    LD_PRELOAD="$(realpath "$(gcc -print-file-name=libasan.so)") $(realpath "$(gcc -print-file-name=libstdc++.so)")"
    export LD_PRELOAD
    echo "Preload AsanSo: LD_PRELOAD=${LD_PRELOAD} (CI_ASAN_BUILD=1)"
    "$@" -A 2>"${AsanFile}.tmp" | tee "$AsanFile"
  elif [[ "${CI_NO_ASAN}" == "1" ]]; then
    echo "Preload AsanSo: skipped (CI_NO_ASAN=1)"
    "$@" 2>"${AsanFile}.tmp" | tee "$AsanFile"
  else
    # 兜底：legacy 路径（本地开发环境）
    LD_PRELOAD="$(realpath "$(gcc -print-file-name=libasan.so)") $(realpath "$(gcc -print-file-name=libstdc++.so)")"
    export LD_PRELOAD
    echo "Preload AsanSo: LD_PRELOAD=${LD_PRELOAD}"
    "$@" -A 2>"${AsanFile}.tmp" | tee "$AsanFile"
  fi
  # Append stderr (ASAN) to psim.info for checkAsan.sh compatibility
  cat "${AsanFile}.tmp" >> "$AsanFile" 2>/dev/null
  rm -f "${AsanFile}.tmp"

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
    sleep 2
    "$TEST_CODE_DIR"/ci/checkAsan.sh
  else
    echo "Execute script failure"
    exit 1
  fi
fi