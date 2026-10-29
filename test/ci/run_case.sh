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
    # taosd only spawns companion binaries that are regular files, so a symlink is skipped.
    rm -f /usr/bin/xnoded || exit 1
    install -m 0755 "${_mock_binary}" /usr/bin/xnoded || exit 1
    test -f /usr/bin/xnoded -a ! -L /usr/bin/xnoded -a -x /usr/bin/xnoded || exit 1
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

# 仅对解码引擎新特性结果集的用例启用 in-tree taos-connector-python（纯 ctypes，源码树
# 可直接 import）：requirements.txt 钉住的 taospy 按 uint16 读 BLOB 长度，>64KB 的 SHOW
# CREATE 结果会被截断，这两个用例必须跑分支内 connector。路径由 run_container.sh 挂载时
# 经 CI_CONNECTOR_PY_DIR 注入（单一事实来源，本脚本不硬编码容器内路径）；
# CI_USE_REPO_CONNECTOR=0 可强制回退发布版；taospy 发布跟上后此块可整体删除。
# $1: 触发场景，仅用于日志。返回码区分三种结果（调用方按码决定是否 WARN，避免
# 「返回 1 有两种含义」的隐式耦合）：0 = 已注入；1 = 显式退出（不打扰）；2 = 不可用。

# 显式关闭判定的单一事实来源：_try_repo_connector 与两个调度分支（文件级/目录级）
# 共用，任何取值口径调整只改这里。仅 0/false/no/off（大小写不敏感）视为关闭；其余
# 任何取值（含常见的 true/yes/on 与空默认）一律视为启用，避免「写成 true 被静默当
# 关闭」只以截断形态失败、难以定位到取值写法问题。
function _repo_connector_disabled() {
    case "${CI_USE_REPO_CONNECTOR:-1}" in
        0|[Ff][Aa][Ll][Ss][Ee]|[Nn][Oo]|[Oo][Ff][Ff]) return 0 ;;
    esac
    return 1
}

function _try_repo_connector() {
    if _repo_connector_disabled; then
        return 1
    fi
    if [ -n "${CI_CONNECTOR_PY_DIR}" ] && [ -d "${CI_CONNECTOR_PY_DIR}/taos" ]; then
        # -d 只看目录存在，挡不住半成品 checkout（稀疏检出/解压中断产生的空 taos/
        # 甚至是合法 namespace package）。注入前做导入探测并断言模块来源：仅
        # `from taos import connect` 成功还不够——若 repo 内 taos/ 缺 __init__.py，
        # Python3 会把它当 namespace package portion 与已装发布版 taospy 的 taos
        # 目录合并，connect 实际来自 taospy（假阳性）。因此同时校验解析出的
        # taos.__file__ 确实位于 CI_CONNECTOR_PY_DIR 之下（目录经环境变量传入，
        # 不拼进 python 源码，路径含引号/空格也不会注入）；import 时即加载
        # libtaos，顺带校验原生库可用。任一不满足按不可用回退发布版，避免目录
        # 级调度让整目录用例以 ImportError 或「>64KB BLOB 被截断」集体失败
        # 探针解释器默认 python3；与用例实际执行的解释器一致性说明：注入机制是
        # PYTHONPATH 前置，对任何解释器都生效且优先于 site-packages（带 __init__.py
        # 的常规包优先命中注入目录），配合上面的 __file__ 来源校验，探测结论在实
        # 践中不随解释器变化。极端场景（pytest 是 venv/conda shim 且 sitecustomize
        # 重排 sys.path）可用 CI_PY_BIN 指定与 pytest 相同的解释器再探。
        # ASan 构建下探针必须与 pytest.sh 相同地预载 libasan/libstdc++：LD_PRELOAD 由
        # pytest.sh 在其内部设置，而探针先于用例命令运行拿不到；不预载时 import taos
        # 会经 CDLL 加载 ASan 版 libtaos 直接以 "ASan runtime does not come first"
        # 中止（进程级 abort，已本机复现），探针把可用的 connector 误判为不可用并触
        # 发下方 fail-fast（正是 CI 上两个 BLOB 用例的失败形态）
        _probe_ld="${LD_PRELOAD:-}"
        if [ "${CI_ASAN_BUILD:-0}" = "1" ]; then
            # realpath can be absent on busybox-ish images; readlink -f is the
            # fallback. An empty resolution is handled by the -n checks below.
            _resolve_lib() { local _p="$1" _r; _r="$(realpath "$_p" 2>/dev/null)" || _r="$(readlink -f "$_p" 2>/dev/null)" || _r=""; printf '%s' "$_r"; }
            _asan_lib="$(_resolve_lib "$(gcc -print-file-name=libasan.so)")"
            _stdcpp_lib="$(_resolve_lib "$(gcc -print-file-name=libstdc++.so)")"
            if [ -n "${_asan_lib}" ]; then
                # libstdc++ 同样判空拼接：gcc 找不到该库时 realpath 得空串，不能拼出
                # 带空隙的 LD_PRELOAD（与 pytest.sh 的环境保持一致）
                _probe_ld="${_asan_lib}${_stdcpp_lib:+ ${_stdcpp_lib}}${_probe_ld:+ ${_probe_ld}}"
            fi
        fi
        if ! _probe_err="$(mktemp "${TMPDIR:-/tmp}/connprobe.XXXXXX")"; then
            # 临时文件创建失败（TMPDIR 不可写等）不应伪装成探针判定：明确 WARN 后按
            # 不可用降级，避免空重定向目标把「有 connector」误报为不可用
            echo "[run_case] WARN: cannot create probe temp file under TMPDIR='${TMPDIR:-/tmp}'; treating in-repo connector as unusable" >&2
            return 2
        fi
        # detect_leaks=0：预载 libasan 后 LSan 会在进程退出时把解释器自身的分配报成
        # 泄漏并以非零码退出，探针只关心能否 import 与来源校验（pytest.sh 靠管道 tee
        # 吞掉该退出码，探针不能）
        # 探针挂 timeout 兜底：import libtaos 阶段若阻塞会拖死整个用例（普通用例有
        # $TIMEOUT_CMD 保护，探针此前没有）；60s 覆盖 ASan 初始化的慢启动。镜像若
        # 缺少 coreutils timeout 则退回无界执行（不劣于引入 timeout 前），避免把
        # 「工具缺失」误判成「connector 不可用」
        _probe_to="timeout 60"
        command -v timeout >/dev/null 2>&1 || _probe_to=""
        if ! CI_PROBE_DIR="${CI_CONNECTOR_PY_DIR}" LD_PRELOAD="${_probe_ld}" \
             ASAN_OPTIONS="detect_leaks=0" \
             PYTHONPATH="${CI_CONNECTOR_PY_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
             ${_probe_to:+$_probe_to} "${CI_PY_BIN:-python3}" -c 'import os, sys, taos
from taos import connect
_d = os.environ["CI_PROBE_DIR"]
_f = getattr(taos, "__file__", None)
sys.exit(0 if _f and os.path.realpath(_f).startswith(os.path.realpath(_d) + os.sep) else 1)' >"${_probe_err}" 2>&1; then
            # 探针 stderr 已被重定向，失败时把末尾摘录带进 WARN，让下次失败可直接定位
            echo "[run_case] WARN: in-repo connector at CI_CONNECTOR_PY_DIR='${CI_CONNECTOR_PY_DIR}' is present but not the importable taos module source; ignoring it; probe tail: $(tail -c 300 "${_probe_err}" | tr "\n" " ")" >&2
            rm -f "${_probe_err}"
            return 2
        fi
        rm -f "${_probe_err}"
        # 清理放在探测块收尾处，覆盖成功/不可用（上方 return 2 前自成清理）之外的全部
        # 后续路径；显式关闭（return 1）不进入本块，无需这些变量
        unset _probe_ld _asan_lib _stdcpp_lib _probe_to
        export PYTHONPATH="${CI_CONNECTOR_PY_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
        echo "[run_case] PYTHONPATH prepended ($1): using in-repo taos-connector-python"
        return 0
    fi
    return 2
}

case "$cmd" in
    # 文件级调度（CI cases.task 的唯一形态）：按「/ + 用例文件名」锚定匹配（.py 结
    # 尾、后跟空格参数或 :: node-id）。要求文件名前必须有 /（cases.task 的调度命令
    # 恒为 cases/... 路径形态），避免裸文件名出现在 -k 过滤表达式等非调度位置时误
    # 判为强依赖（误判会走下方 fail-fast 直接失败）；作用域刻意收窄，避免 in-tree
    # connector 与发布版的无关差异波及其它历史用例，也避免误命中同前缀文件（如
    # .py_bak）。--deselect cases/.../x.py 仍会命中（与真实调度共用路径形态），属
    # 已知残余，cases.task 中未使用该形态。
    */test_show_create_table_blob.py|*/test_show_create_table_blob.py\ *|*/test_show_create_table_blob.py:*|\
    */test_taosdump_long_ddl.py|*/test_taosdump_long_ddl.py\ *|*/test_taosdump_long_ddl.py:*)
        # 回退到发布版属于配置缺陷而非预期路径：这两个用例的前提就是发布版 taospy 按
        # uint16 截断 >64KB BLOB，静默回退只会以“结果被截断”的形态失败，与真实代码
        # 缺陷无法区分。判定顺序刻意是：显式关闭（逃生开关，走 _repo_connector_disabled
        # 与 _try_repo_connector 同一判定）先于「未挂载」fail-fast，否则
        # CI_CONNECTOR_PY_DIR 未设 + CI_USE_REPO_CONNECTOR=0 的组合会让逃生开关失效。
        # 未挂载时按场景分流：CI 容器内（有 CI_PROJECT_DIR）fail-fast 报配置错误；
        # 主机侧无 CI 环境的单用例重跑只 WARN 并继续跑发布版——直接 abort 且无任何
        # 测试输出，比发布版可能产生的截断失败更难排查。connector 已挂载但探针判定
        # 不可用才是配置错误，fail-fast 并点名成因；目录级分支见下（可能未选中这些
        # 用例，保留 WARN）。
        if _repo_connector_disabled; then
            echo "[run_case] NOTE: CI_USE_REPO_CONNECTOR disabled the in-repo connector; running with the published taospy (fine once its BLOB decode catches up)" >&2
        elif [ -z "${CI_CONNECTOR_PY_DIR:-}" ]; then
            if [ -n "${CI_PROJECT_DIR:-}" ]; then
                # 成因一（CI 容器内）：宿主侧候选探测全部落空、connector 根本没挂
                # 进来。对强依赖用例 fail-fast：继续跑发布版只会以截断形态失败、
                # 与真实缺陷不可区分，且 fail-fast 的明确报错曾让 ASan 探针问题被
                # 快速定位（WARN 会让排查方向错落到用例本身）。CI_PROJECT_DIR 由
                # GitLab CI 注入，作为容器内运行的判据。
                echo "[run_case] ERROR: this case requires the in-repo connector but none is mounted (CI_CONNECTOR_PY_DIR unset; run_container.sh host-side probe found no connector); aborting instead of running it against the published taospy, whose uint16 BLOB length reads would only produce misleading truncation failures" >&2
                exit 1
            fi
            # host-side single-case rerun (rerun.sh) without any CI env: warn and
            # keep going with the published connector -- aborting the rerun with
            # exit 1 and no test output is less diagnosable than the truncation
            # failures the published taospy may produce here
            echo "[run_case] WARN: host-side rerun without CI_CONNECTOR_PY_DIR; running with the published taospy (BLOB cases may fail with truncated results)" >&2
        else
            _try_repo_connector "case requires BLOB-aware connector"
            # 立即固化返回码再判断：$? 会被中间任何命令覆盖
            _conn_rc=$?
            if [ ${_conn_rc} -eq 2 ]; then
                # 成因二：connector 已挂载（CI_CONNECTOR_PY_DIR 非空）但探针判定
                # 不可用（半成品 checkout / 命名空间包 / 库加载失败等）——对强依赖
                # 用例这才是配置错误，fail-fast。注意 _try_repo_connector 的裸
                # return 2 只在变量未设时触发，此分支内不可达
                echo "[run_case] ERROR: this case requires the in-repo connector but the mounted CI_CONNECTOR_PY_DIR='${CI_CONNECTOR_PY_DIR}' is unusable; aborting instead of running it against the published taospy, whose uint16 BLOB length reads would only produce misleading truncation failures" >&2
                exit 1
            fi
        fi
        ;;
    # 目录级/-k 调度：目录名后允许字符串结尾、/、空格或 / 再接参数。注意带 / 的模式
    # 会连带命中「目录/文件」形态——cases.task 的文件级命令（pytest cases/23-ShowCommands/
    # test_show_transaction.py）同样含子串 cases/23-ShowCommands/，故分支内再用内层
    # case 排除指向具体 .py 的命令：两个 BLOB 用例已被上方文件级分支先行处理，其余
    # .py 是无关历史用例，注入 in-tree connector 只会引入与发布版 taospy 的行为差异。
    # 整目录（含尾斜杠）或 -k 过滤必然覆盖目录内的 BLOB 用例，只 WARN 无法阻止其以
    # 截断形态失败，故仍前置 connector；注入不可用且未显式退出时才 WARN（走 stderr）。
    *cases/23-ShowCommands|*cases/23-ShowCommands/|*cases/23-ShowCommands\ *|*cases/23-ShowCommands/\ *|\
    *cases/81-Tools/04-Taosdump|*cases/81-Tools/04-Taosdump/|*cases/81-Tools/04-Taosdump\ *|*cases/81-Tools/04-Taosdump/\ *)
        case "$cmd" in
            *cases/23-ShowCommands/[![:space:]]*.py|*cases/23-ShowCommands/[![:space:]]*.py:*|\
            *cases/81-Tools/04-Taosdump/[![:space:]]*.py|*cases/81-Tools/04-Taosdump/[![:space:]]*.py:*)
                # 文件级调度（非 BLOB 用例）：目录后紧跟无空格文件名才是该形态，
                # 含 ::node-id 变体（与外层文件级分支的 .py:* 形态对齐）。不能写成
                # */dir/*.py——* 可跨空格，会把「目录 + -k 引用某 .py 文件名」的目
                # 录级调度误判为文件级而跳过注入，令被 -k 选中的 BLOB 用例静默跑发
                # 布版（截断形态失败）。[![:space:]] 要求目录斜杠后第一个字符非空
                # 白，恰好区分两种形态（注意不能用 [! ]：case 模式里的空格会切断
                # 模式词导致语法错误，POSIX 字符类无此问题）
                ;;
            *)
                _try_repo_connector "directory-level scheduling"
                # 同上：固化返回码，避免插入语句覆盖 $? 使 WARN 失效
                _conn_rc=$?
                if [ ${_conn_rc} -eq 1 ]; then
                    # 与文件级分支对称：显式关闭时留一行线索，避免排查时误以为目录级
                    # 调度不走注入逻辑
                    echo "[run_case] NOTE: CI_USE_REPO_CONNECTOR=0 disabled the in-repo connector; running with the published taospy (fine once its BLOB decode catches up)" >&2
                fi
                if [ ${_conn_rc} -eq 2 ]; then
                    # -k 等过滤条件可能并未选中 BLOB 用例，WARN 措辞保持条件式，避免误报方向
                    echo "[run_case] WARN: directory/-k run under a directory containing test_show_create_table_blob.py / test_taosdump_long_ddl.py, but CI_CONNECTOR_PY_DIR='${CI_CONNECTOR_PY_DIR:-}' is unusable; IF this run selects them, their >64KB BLOB assertions will likely fail" >&2
                fi
                ;;
        esac
        ;;
esac

_ci_command_start_ms=$(date +%s%3N)
echo "[ci-timing] stage=container-preparation elapsed_ms=$(( _ci_command_start_ms - _ci_entry_start_ms ))"
echo "[ci-timing] stage=test-command event=start"
$TIMEOUT_CMD bash -c "$cmd"
RET=$?
echo "[ci-timing] stage=test-command elapsed_ms=$(( $(date +%s%3N) - _ci_command_start_ms )) rc=${RET}"
# pytest exit code 5 = no tests collected / all skipped → treat as pass
[ $RET -eq 5 ] && RET=0
echo "cmd exit code: $RET"

# Publish only after the test command returned. The EXIT trap alone is not
# proof of completion: it can also run when a timed-out container is stopped.
if [[ -n "${CI_TEST_COMMAND_EXIT_MARKER:-}" ]]; then
    printf '%s %s\n' "$(date +%s%3N)" "${RET}" > "${CI_TEST_COMMAND_EXIT_MARKER}.tmp"
    mv -f "${CI_TEST_COMMAND_EXIT_MARKER}.tmp" "${CI_TEST_COMMAND_EXIT_MARKER}"
fi

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
