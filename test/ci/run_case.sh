#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# Publish the entrypoint exit time before Docker performs --rm teardown.
_ci_entry_start_ms=$(date +%s%3N)
_ci_entry_exit() {
    local rc=$?
    local end_ms=$(date +%s%3N)
    echo "[ci-timing] stage=container-entrypoint elapsed_ms=$(( end_ms - _ci_entry_start_ms )) rc=${rc}"
    if [[ -n "${CI_CONTAINER_EXIT_MARKER:-}" ]]; then
        printf '%s %s\n' "${end_ms}" "${rc}" > "${CI_CONTAINER_EXIT_MARKER}"
    fi
}
trap _ci_entry_exit EXIT

function usage() {
    echo "$0"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -h help"
}

ent=0
while getopts "d:c:o:eh" opt; do
    case $opt in
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
            ;;
        o)
            TIMEOUT_CMD="timeout $OPTARG"
            ;;
        e)
            ent=1
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done

if [ -z "$exec_dir" ]; then
    usage
    exit 0
fi

if [ -z "$cmd" ]; then
    usage
    exit 0
fi

# handle the exec_dir is "." specially
if [ "$exec_dir" = "." ]; then
    target_dir="test"
else
    target_dir="test/$exec_dir"
fi

# ── 自动检测容器内路径布局 ─────────────────────────────────────────────────
# tsdb 仓库 CI：/mnt/tsdb/debug/build/bin, /mnt/tsdb/source/taos-community
# TDinternal CI：/home/TDinternal/debug/build/bin, /home/TDinternal/community
# TDengine OSS CI：/home/TDengine/debug/build/bin, /home/TDengine
if [ -d "/mnt/tsdb/debug/build/bin" ]; then
    _DEBUG_BASE="/mnt/tsdb/debug"
    _SOURCE_BASE="/mnt/tsdb/source/taos-community"
    _SIM_BASE="/mnt/tsdb/sim"
elif [ -d "/home/TDinternal/debug/build/bin" ]; then
    _DEBUG_BASE="/home/TDinternal/debug"
    _SOURCE_BASE="/home/TDinternal/community"
    _SIM_BASE="/home/TDinternal/sim"
else
    _DEBUG_BASE="/home/TDengine/debug"
    _SOURCE_BASE="/home/TDengine"
    _SIM_BASE="/home/TDengine/sim"
fi

export PATH=$PATH:${_DEBUG_BASE}/build/bin
export LD_LIBRARY_PATH=${_DEBUG_BASE}/build/lib
if [[ -n "${XNODED_TEST_MOCK_MODE:-}" ]]; then
    _mock_source="${_SOURCE_BASE}/test/ci/mock_xnoded.c"
    _mock_dir="$(mktemp -d "${TMPDIR:-/tmp}/xnoded-mock.XXXXXX")" || exit 1
    _mock_binary="${_mock_dir}/xnoded"
    if ! gcc -std=c11 -O2 -Wall -Wextra -Werror "${_mock_source}" -o "${_mock_binary}"; then
        rm -rf "${_mock_dir}"
        echo "ERROR: failed to build mock xnoded: ${_mock_source}"
        exit 1
    fi
    ln -sf "${_mock_binary}" /usr/bin/xnoded || exit 1
    test -x /usr/bin/xnoded || exit 1
    echo "[xnode-ci] mock xnoded: ${_mock_binary}, mode=${XNODED_TEST_MOCK_MODE}"
fi
ln -s ${_DEBUG_BASE}/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
# 兼容测试（如 test_new_stream_compatibility.py）硬编码 /usr/bin/taos，
# 在此创建符号链接使其生效，无论二进制实际安装位置如何。
ln -sf ${_DEBUG_BASE}/build/bin/taos /usr/bin/taos 2>/dev/null || true
ln -s ${_DEBUG_BASE}/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosws.so /usr/lib/libtaosws.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosws.so /usr/lib/libtaosws.so.1 2>/dev/null
ln -s ${_SOURCE_BASE}/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null
ln -s ${_DEBUG_BASE}/include/taosws.h /usr/include/taosws.h 2>/dev/null
# 刷新系统动态链接器缓存
echo "${_DEBUG_BASE}/build/lib" > /etc/ld.so.conf.d/tdengine.conf
ldconfig 2>/dev/null || true
CONTAINER_TESTDIR=${_SOURCE_BASE}

mkdir -p /var/lib/taos/subscribe
mkdir -p /var/log/taos
mkdir -p /var/lib/taos
mkdir -p /etc/taos

# Compatibility shim: some tests grep "SET(TD_VER_NUMBER " from version.cmake,
# but the variable was renamed to BUILD_VER_NUMBER. Append an alias line so
# both old and new grep patterns work, without modifying individual test files.
_ver_cmake="${CONTAINER_TESTDIR}/cmake/version.cmake"
if [ -f "${_ver_cmake}" ] && ! grep -q "SET(TD_VER_NUMBER " "${_ver_cmake}"; then
    _ver_val=$(grep -oP 'SET\(BUILD_VER_NUMBER "\K[^"]+' "${_ver_cmake}" | head -1)
    if [ -n "${_ver_val}" ]; then
        echo "SET(TD_VER_NUMBER \"${_ver_val}\")" >> "${_ver_cmake}"
    fi
fi

cd $CONTAINER_TESTDIR/$target_dir || { echo "Can't enter the target dirctory: ${CONTAINER_TESTDIR}/${target_dir}"; exit 1; }
ulimit -c unlimited

# 使用容器本地 marker + requirements.txt 内容 hash 比较，智能安装依赖。
# marker 不能放在 bind-mounted 源码目录里；CI 每个 case 是新容器，
# 但源码目录可能跨容器共享，旧 marker 会让新容器跳过 pip install。
# tdengine-ci:0.3 image bakes Nexus as default pip index; no -i needed
_REQ_FILE="${CONTAINER_TESTDIR}/test/requirements.txt"
_REQ_HASH=$(python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest()[:16])' "${_REQ_FILE}" 2>/dev/null || echo "unknown")
_PY_TAG=$(python3 -c 'import sys; print(f"{sys.implementation.cache_tag}-{sys.version_info[0]}.{sys.version_info[1]}")' 2>/dev/null || echo "python")
_DEPS_MARKER="${TMPDIR:-/tmp}/tdengine-ci-deps-${_PY_TAG}-${_REQ_HASH}.installed"
if [[ ! -f "${_DEPS_MARKER}" ]] || [[ "${_REQ_FILE}" -nt "${_DEPS_MARKER}" ]]; then
    _PIP_LOG=$(mktemp "${TMPDIR:-/tmp}/tdengine-ci-pip.XXXXXX")
    if pip3 install -r "${_REQ_FILE}" -q >"${_PIP_LOG}" 2>&1; then
        tail -5 "${_PIP_LOG}"
        touch "${_DEPS_MARKER}"
    else
        tail -50 "${_PIP_LOG}"
        rm -f "${_PIP_LOG}"
        exit 1
    fi
    rm -f "${_PIP_LOG}"
fi

_ci_command_start_ms=$(date +%s%3N)
echo "[ci-timing] stage=container-preparation elapsed_ms=$(( _ci_command_start_ms - _ci_entry_start_ms ))"
echo "[ci-timing] stage=test-command event=start"
$TIMEOUT_CMD $cmd
RET=$?
echo "[ci-timing] stage=test-command elapsed_ms=$(( $(date +%s%3N) - _ci_command_start_ms )) rc=${RET}"
# pytest exit code 5 = no tests collected / all skipped → treat as pass
[ $RET -eq 5 ] && RET=0
echo "cmd exit code: $RET"

mkdir -p ${_SIM_BASE}/var_taoslog
if [ -d "/var/log/taos" ]; then
    cp /var/log/taos/* ${_SIM_BASE}/var_taoslog/ 2>/dev/null || true
fi

if [ -f "${CONTAINER_TESTDIR}/docs/examples/java/jdbc-out.log" ]; then
    cp ${CONTAINER_TESTDIR}/docs/examples/java/jdbc-out.log ${_SIM_BASE}/var_taoslog/ 2>/dev/null || true
fi

if [ $RET -ne 0 ]; then
    pwd
fi

exit $RET
