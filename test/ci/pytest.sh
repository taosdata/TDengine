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
export PATH="${BUILD_DIR}/build/bin${PATH:+:${PATH}}"

if [ -d "${BUILD_DIR}/build/lib" ]; then
  export LD_LIBRARY_PATH="${BUILD_DIR}/build/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
  echo "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
fi

FQ_RUNTIME_HELPER="${TEST_CODE_DIR}/cases/09-DataQuerying/19-FederatedQuery/fq_runtime_libs.sh"
if [ -f "${FQ_RUNTIME_HELPER}" ]; then
  # shellcheck source=/dev/null
  source "${FQ_RUNTIME_HELPER}"
  fq_runtime_libs_setup "${BUILD_DIR}" "$@"
fi

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
# Ensure a high open-files limit for the test client. The pytest process opens
# many TDengine connections (each self.login() reconnects to every dnode) plus
# allure result files; a low fd ceiling causes two failure modes under parallel
# CI + ASAN, both rooted in fd exhaustion:
#   - client socket/fd exhaustion -> 0x012f "third party error" at connect time
#   - pytest/allure -> "OSError: [Errno 24] Too many open files" aborting the
#     whole session with an INTERNALERROR
# Previously `ulimit -S -n 600000` failed with EINVAL ("cannot modify limit:
# Invalid argument") whenever the hard limit was below 600000, silently leaving
# the soft limit at its low default. This container runs --privileged, so raise
# the hard limit first (best effort), then set the soft limit clamped to
# whatever the hard limit actually allows.
want_nofile=1048576
ulimit -H -n "$want_nofile" 2>/dev/null || true
hard_nofile=$(ulimit -H -n)
if [ "$hard_nofile" = "unlimited" ] || [ "$hard_nofile" -gt "$want_nofile" ]; then
  ulimit -S -n "$want_nofile"
else
  ulimit -S -n "$hard_nofile"
fi
echo "open files limit: soft=$(ulimit -S -n) hard=$(ulimit -H -n)"
ulimit -c unlimited

# sudo sysctl -w kernel.core_pattern=$TOP_DIR/core.%p.%e
echo "ExcuteCmd:" "$@"

if [[ "$TD_OS" == "Alpine" ]]; then
  "$@"
else
  AsanFile="${ASAN_DIR}/psim.info"
  echo "AsanFile:" "$AsanFile"

  unset LD_PRELOAD
  AsanStdoutTmp=$(mktemp "${TMPDIR:-/tmp}/psim.stdout.XXXXXX") || exit 1
  AsanStderrTmp=$(mktemp "${TMPDIR:-/tmp}/psim.stderr.XXXXXX") || exit 1
  if [[ "${CI_ASAN_BUILD}" == "1" ]]; then
    # ASAN 构建（others:latest GCC14, libasan.so.8 与测试容器匹配）
    LD_PRELOAD="$(realpath "$(gcc -print-file-name=libasan.so)") $(realpath "$(gcc -print-file-name=libstdc++.so)")"
    export LD_PRELOAD
    echo "Preload AsanSo: LD_PRELOAD=${LD_PRELOAD} (CI_ASAN_BUILD=1)"
    "$@" -A 2>"${AsanStderrTmp}" | tee "${AsanStdoutTmp}"
  elif [[ "${CI_NO_ASAN}" == "1" ]]; then
    echo "Preload AsanSo: skipped (CI_NO_ASAN=1)"
    "$@" 2>"${AsanStderrTmp}" | tee "${AsanStdoutTmp}"
  else
    # 兜底：legacy 路径（本地开发环境）
    LD_PRELOAD="$(realpath "$(gcc -print-file-name=libasan.so)") $(realpath "$(gcc -print-file-name=libstdc++.so)")"
    export LD_PRELOAD
    echo "Preload AsanSo: LD_PRELOAD=${LD_PRELOAD}"
    "$@" -A 2>"${AsanStderrTmp}" | tee "${AsanStdoutTmp}"
  fi
  # pytest --clean may recreate SIM_DIR while tee is running, so stage output
  # outside SIM_DIR and publish it after pytest exits.
  mkdir -p "${ASAN_DIR}"
  cat "${AsanStdoutTmp}" > "$AsanFile" 2>/dev/null
  cat "${AsanStderrTmp}" >> "$AsanFile" 2>/dev/null
  rm -f "${AsanStdoutTmp}" "${AsanStderrTmp}"

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
