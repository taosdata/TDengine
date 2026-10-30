#!/bin/bash
# 兼容性约束：本脚本需在 bash < 4.4（老 CI 镜像为 bash 4.2.x）下工作。下文数组展开
# 统一写作 ${arr[@]+"${arr[@]}"} ——空数组时 "${arr[@]}" 在 bash <4.4 会注入一个空
# 参数（docker 会把它当作镜像名而失败），请勿按 bash 4.4+ 习惯“简化”该写法。

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

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -t thread number"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -s build with sanitizer"
    echo -e "\t -h help"
}

ent=0
container_name=""
while getopts "w:d:c:t:n:o:s:eh" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
            ;;
        t)
            thread_no=$OPTARG
            ;;
        n)
            container_name=$OPTARG
            ;;
        e)
            ent=1
            ;;
        o)
            extra_param="-o $OPTARG"
            ;;
        s)
            buildSan=$OPTARG
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

if [ -z "$WORKDIR" ]; then
    usage
    exit 1
fi
if [ -z "$exec_dir" ]; then
    usage
    exit 1
fi
if [ -z "$cmd" ]; then
    usage
    exit 1
fi
if [ -z "$thread_no" ]; then
    usage
    exit 1
fi

#select whether the compilation environment  includes sanitizer
if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
elif [[ "${buildSan}" == "n" ]] || [[ "${case_build_san}" == "" ]]; then
    DEBUGPATH="debugNoSan"
else
    usage
    exit 1
fi

# 容器内路径布局，区分两种 CI 环境：
#   TDinternal CI（$WORKDIR/TDinternal/enterprise 存在）：
#     企业版 → 挂载整个 TDinternal 到 /home/TDinternal（包含 enterprise/、community/）
#     社区版 → 挂载 TDengine 到 /home/TDengine
#   tsdb CI（无 enterprise/ 目录）：
#     统一挂载到 /mnt/tsdb/source/taos-community、/mnt/tsdb/debug、/mnt/tsdb/sim
#   run_case.sh 通过自动检测选择容器内对应路径。
# 连接器挂载/环境变量在布局分支前统一初始化，保证两条 docker run 在任何布局下取值确定；
# SCRIPT_DIR（脚本位置反推）供各布局分支探测同级 connector 使用，兼容 sparse-checkout
# 创建的 symlink，且不依赖 REPDIR 的回退赋值路径
REP_MOUNT_CONNECTOR=""
CONNECTOR_PY_ENV_ARG=""
# 各布局分支探测前统一初始化（未命中保持空串），与上面两个变量保持同一不变量：
# 分支外引用（日志/断言）不依赖某条分支是否执行过
CONNECTOR_PY_HOST=""
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# connector 相对路径按仓库家族区分：TDinternal/TDengine 检出的 connector 在仓库根，
# tsdb monorepo 检出在 source/ 下——布局不同是预期（CI_PROJECT_DIR 在不同流水线指向
# 不同仓库），不是笔误。两个值都是相对各自 checkout 根的路径；INTERNAL 同时用作企业版
# 分支探测循环里的目录名（根或 community/ 下）。
CONNECTOR_PY_REL_INTERNAL="taos-connector-python"
CONNECTOR_PY_REL_TSDB="source/taos-connector-python"
# 探测候选前缀单一事实来源（裸名 / community/ / source/），企业版与社区版两个
# 探测循环共用——两处各写一份字面量时，新增布局前缀漏改其一会让该分支静默探测
# 不到 connector，下游 run_case.sh 对 BLOB 用例是 fail-fast，排障成本高
_CONNECTOR_CANDIDATES=("${CONNECTOR_PY_REL_INTERNAL}" \
                       "community/${CONNECTOR_PY_REL_INTERNAL}" \
                       "${CONNECTOR_PY_REL_TSDB}")
if [ -d "$WORKDIR/TDinternal/enterprise" ]; then
    # ── TDinternal CI 布局（同步回 TDinternal 后原样工作）────────────────
    if [ $ent -ne 0 ]; then
        extra_param="$extra_param -e"
        INTERNAL_REPDIR=$WORKDIR/TDinternal
        REPDIR=$INTERNAL_REPDIR/community
        REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
        CONTAINER_TESTDIR=/home/TDinternal/community
        SIM_DIR=/home/TDinternal/sim
        REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"
        REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
        REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
        # 整个 TDinternal 已挂载到 /home/TDinternal，仓库内带 taos-connector-python 时
        # 只需注入路径（无需额外 -v）；不存在时保持空，run_case.sh 对两个 BLOB 强依赖
        # 用例 fail-fast 报明确配置错误（目录级/-k 调度仍为 WARN）。
        # 与下方 tsdb 分支对称地多位置探测：connector 可能位于 TDinternal 根或
        # community/ 下（布局演进），命中后直接用记录的相对片段拼容器内路径——不做
        # 字符串前缀剥离，避免 WORKDIR 尾斜杠/符号链接导致映射错误；都未命中才回退。
        CONNECTOR_PY_HOST=""
        CONNECTOR_PY_REL=""
        # 候选列表与社区版/tsdb 分支统一（裸名/community//source/ 三前缀对称，布局
        # 误判进入对方分支时仍能命中）；命中条件附 /taos 子目录校验（真 python 包形
        # 态），过滤恰同名但非 connector 的残留目录，与 run_case.sh 侧
        # -d ${CI_CONNECTOR_PY_DIR}/taos 的校验口径一致
        for _cand in "${_CONNECTOR_CANDIDATES[@]}"; do
            if [ -f "$INTERNAL_REPDIR/$_cand/taos/__init__.py" ]; then
                CONNECTOR_PY_HOST="$INTERNAL_REPDIR/$_cand"
                CONNECTOR_PY_REL="$_cand"
                break
            fi
        done
        unset _cand
        # 与社区版/tsdb 分支同一不变量：以 HOST 非空 + -d 双重校验为门（REL 仅用于拼
        # 容器内路径，不作存在性依据），避免「REL 非空但目录不存在」的残留态把不存
        # 在的目录注入 CI_CONNECTOR_PY_DIR（下游会对 BLOB 用例 fail-fast）
        if [ -n "$CONNECTOR_PY_HOST" ] && [ -d "$CONNECTOR_PY_HOST" ]; then
            echo "[run_container] connector detected: $CONNECTOR_PY_HOST"
            CONNECTOR_PY_ENV_ARG="CI_CONNECTOR_PY_DIR=/home/TDinternal/${CONNECTOR_PY_REL}"
        fi
    else
        REPDIR=$WORKDIR/TDengine
        REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
        CONTAINER_TESTDIR=/home/TDengine
        SIM_DIR=/home/TDengine/sim
        REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
        REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
        REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDengine/debug/build/lib:ro"
        # 与企业版分支对称：按「基准目录 × 相对候选」列表探测，命中即挂入容器并
        # 注入路径。脚本位置可能处于不同家族仓库的子树（TDinternal/community、
        # TDengine、甚至 tsdb 的 source/taos-community——由上方 enterprise 目录
        # 存在性误判进入），connector 相对 checkout 根的位置随之在「根 / source/
        # 下 / 仓库同级」之间变化，因此主基准取脚本位置反推的父目录（TDinternal
        # 根、TDengine 的 WORKDIR、tsdb 的 source/），兜底再探 CI_PROJECT_DIR
        # （runner 完整 checkout 根）及其父目录（同级布局），各基准下都尝试裸名
        # 与 source/ 前缀两种候选；探测不到则保持空，下游 run_case.sh 对两个 BLOB
        # 用例的失败形态按场景分流：CI 容器内（有 CI_PROJECT_DIR）为 fail-fast
        # 配置错误，主机侧无 CI 环境的重跑只打 WARN 后继续跑发布版 connector
        CONNECTOR_PY_HOST=""
        # 基准 x 候选存在刻意的「错位一层」组合：_repo_root（脚本上溯两级）在本分
        # 支服务的两种布局下恰等于 REPDIR（TDengine: <repo>/test/ci 上溯两级=
        # <repo>；TDinternal 非企业版: TDinternal/community）——connector 位于仓
        # 库根时由此基准的裸名候选命中（此前只探其父目录、漏掉该层）；_conn_base
        # 取其父（tsdb 误入时即 source/，由裸名候选命中）。三种前缀是穷举保险，勿
        # 按「基准已含某层」的直觉删改候选顺序
        _repo_root="$(cd "${SCRIPT_DIR}/../.." && pwd)"
        _conn_base="$(dirname "$_repo_root")"
        # 空串元素（CI_PROJECT_DIR 未设时）由 [ -n ] 跳过；候选三前缀与企业版分支
        # 对称（含 community/——TDinternal 布局误入本分支时仍可命中）；/taos 校验
        # 过滤同名残留目录
        # 去尾斜杠归一化：CI_PROJECT_DIR 以 / 结尾时 dirname 会多剥一层，把上两层
        # 当作 checkout 根探测（GitLab 通常不带尾斜杠，但不应依赖该外部约定）
        _ci_root="${CI_PROJECT_DIR%/}"
        for _base in "$_repo_root" "$_conn_base" "${_ci_root:-}" "${_ci_root:+$(dirname "$_ci_root")}"; do
            # 仅接受绝对路径基准：cd 失败时基准会退化为 "."（相对），不排除会
            # 把相对路径当宿主机挂载源交给 docker -v（由 daemon 按其 cwd 解析，挂错）
            case "$_base" in /*) ;; *) continue ;; esac
            for _cand in "${_CONNECTOR_CANDIDATES[@]}"; do
                if [ -f "${_base}/$_cand/taos/__init__.py" ]; then
                    CONNECTOR_PY_HOST="${_base}/$_cand"
                    break 2
                fi
            done
        done
        unset _base _cand _conn_base _repo_root _ci_root
        if [ -n "$CONNECTOR_PY_HOST" ] && [ -d "$CONNECTOR_PY_HOST" ]; then
            REP_MOUNT_CONNECTOR="${CONNECTOR_PY_HOST}:/home/taos-connector-python:ro"
            CONNECTOR_PY_ENV_ARG="CI_CONNECTOR_PY_DIR=/home/taos-connector-python"
        fi
    fi
else
    # ── tsdb CI 布局 ────────────────────────────────────────────────────
    # tsdb 仓库原生路径：脚本自身位于 source/taos-community/test/ci/run_container.sh
    # 优先从脚本位置反推，兼容 sparse-checkout 创建的 symlink
    [ $ent -ne 0 ] && extra_param="$extra_param -e"
    TSDB_COMMUNITY=$(cd "${SCRIPT_DIR}/../.." && pwd)  # → source/taos-community
    if [ -d "${TSDB_COMMUNITY}/test" ] && [ -d "${TSDB_COMMUNITY}/source" ]; then
        REPDIR=${TSDB_COMMUNITY}
    elif [ -d "$WORKDIR/TDengine" ]; then
        REPDIR=$WORKDIR/TDengine
    elif [ -d "$WORKDIR/TDinternal/community" ]; then
        REPDIR=$WORKDIR/TDinternal/community
    else
        echo "ERROR: Cannot find source directory under $WORKDIR"
        exit 1
    fi
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/mnt/tsdb/source/taos-community
    SIM_DIR=/mnt/tsdb/sim
    REP_MOUNT_PARAM="${REPDIR}:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
    # 若 taos-internal 目录与 taos-community 同级（tsdb / TDinternal 布局），一并挂入容器，
    # 使企业版测试用例（如 test_new_stream_compatibility.py）可访问 /mnt/tsdb/source/taos-internal
    TAOS_INTERNAL_DIR="$(dirname "$REPDIR")/taos-internal"
    # 兜底：同级目录不存在时，尝试 CI_PROJECT_DIR/source/taos-internal（GitLab runner 完整 checkout）
    if [ ! -d "$TAOS_INTERNAL_DIR" ] && [ -n "${CI_PROJECT_DIR:-}" ] && \
       [ -d "${CI_PROJECT_DIR}/source/taos-internal" ]; then
        TAOS_INTERNAL_DIR="${CI_PROJECT_DIR}/source/taos-internal"
    fi
    REP_MOUNT_INTERNAL=""
    if [ -d "$TAOS_INTERNAL_DIR" ]; then
        REP_MOUNT_INTERNAL="${TAOS_INTERNAL_DIR}:/mnt/tsdb/source/taos-internal"
    fi
    # taos-connector-python 同理挂入容器：run_case.sh 仅对依赖新 BLOB 解码的两个用例
    # （test_show_create_table_blob.py / test_taosdump_long_ddl.py，按 case "$cmd" 匹配）
    # 前置其 PYTHONPATH，让这些用例跑 in-tree connector 而非 requirements.txt 钉住的
    # 发布版 taospy。engine 与 connector 在本仓库同步演进（如 BLOB 列 4 字节长度头），
    # 发布版滞后时只有分支内 connector 才能正确解码新特性结果。容器内路径经
    # CI_CONNECTOR_PY_DIR 传给 run_case.sh，挂载目标只在本处定义一次（run_case.sh
    # 不再硬编码，避免两侧静默失配）。
    # 推导：connector 与 taos-community 同级（tsdb monorepo 原生布局）。锚定脚本位置
    # 反推的 TSDB_COMMUNITY（= source/taos-community）取同级目录，而非 dirname "$REPDIR"
    # ——REPDIR 可能经上方回退落入 $WORKDIR/TDengine、$WORKDIR/TDinternal/community 等
    # 布局，dirname 会得到 $WORKDIR（或 $WORKDIR/TDinternal），若那里恰有残留的
    # taos-connector-python 目录就会把错误的 connector 挂入容器。脚本位置反推的根
    # 与本脚本自身强绑定，不受 REPDIR 回退影响；未命中再经 CI_PROJECT_DIR（GitLab
    # runner 完整 checkout 的仓库根）兜底；都未命中则回退发布版 taospy，run_case.sh
    # 会对两个 BLOB 强依赖用例 fail-fast 报明确配置错误（目录级/-k 调度仍为 WARN）。
    # REP_MOUNT_CONNECTOR/CONNECTOR_PY_ENV_ARG 已在布局分支前统一初始化，此处不再重置。
    # 校验口径与 run_case.sh 对齐（taos/__init__.py 存在，过滤同名残留与半成品
    # checkout 的 namespace package 目录），并补 detected 日志——否则排障时无法区分
    # 「未检出 connector」与「检出后被下游探针否定」
    TAOS_CONNECTOR_PY_DIR="$(dirname "$TSDB_COMMUNITY")/taos-connector-python"
    # 仅接受绝对路径（与社区版分支的基准守卫对称），且必须在下方 -f 判断之前：cd 失败
    # 退化的相对路径若 cwd 恰有同名目录会令 ! -f 短路、跳过 CI_PROJECT_DIR 兜底
    case "$TAOS_CONNECTOR_PY_DIR" in /*) ;; *) TAOS_CONNECTOR_PY_DIR="" ;; esac
    if [ -z "$TAOS_CONNECTOR_PY_DIR" ] && [ -n "${CI_PROJECT_DIR:-}" ] && \
       [ -f "${CI_PROJECT_DIR}/${CONNECTOR_PY_REL_TSDB}/taos/__init__.py" ]; then
        TAOS_CONNECTOR_PY_DIR="${CI_PROJECT_DIR}/${CONNECTOR_PY_REL_TSDB}"
    fi
    if [ -n "$TAOS_CONNECTOR_PY_DIR" ] && [ -f "$TAOS_CONNECTOR_PY_DIR/taos/__init__.py" ]; then
        echo "[run_container] connector detected: $TAOS_CONNECTOR_PY_DIR"
        REP_MOUNT_CONNECTOR="${TAOS_CONNECTOR_PY_DIR}:/mnt/tsdb/source/taos-connector-python:ro"
        CONNECTOR_PY_ENV_ARG="CI_CONNECTOR_PY_DIR=/mnt/tsdb/source/taos-connector-python"
    fi
    # 若 REPDIR 下 docs/ 目录不存在（如 TDinternal/community 布局下 docs 位于仓库根目录），
    # 检测同级 docs/ 并追加挂载，使容器内 getTDenginePath()/docs/ 路径可达（test_check_error_code.py 等用例依赖此路径）。
    REP_MOUNT_DOCS=""
    if [ ! -d "${REPDIR}/docs" ]; then
        _parent_docs="$(dirname "$REPDIR")/docs"
        if [ -d "$_parent_docs" ]; then
            REP_MOUNT_DOCS="${_parent_docs}:${CONTAINER_TESTDIR}/docs"
        fi
    fi
fi

ulimit -c unlimited

TMP_DIR=$WORKDIR/tmp
MOUNT_DIR=""
# packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
_CI_SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=compat_packages_mount.sh
source "${_CI_SCRIPT_DIR}/compat_packages_mount.sh"
SOURCEDIR=$(prepare_compat_packages_sourcedir "$WORKDIR")

source "$REPDIR/test/cases/09-DataQuerying/19-FederatedQuery/fq_runtime_libs.sh"

function _needs_fq_ext_env_cache() {
    local case_cmd="$1"
    case "$case_cmd" in
        *19-FederatedQuery*|*03-ExtSource*|*ensure_ext_env*) return 0 ;;
        *) return 1 ;;
    esac
}

function _prepare_fq_mysql80_shared_cache() {
    local cache_dir="$1"
    local tarball_name="fq-mysql-8.0.tar.xz"
    local minimal_name="mysql-8.0.45-linux-glibc2.17-x86_64-minimal.tar.xz"
    local dest="${cache_dir}/${tarball_name}"
    local url="${FQ_CI_MYSQL80_TARBALL_URL:-https://cdn.mysql.com/Downloads/MySQL-8.0/${minimal_name}}"

    mkdir -p "$cache_dir" || return 1
    if [ -s "$dest" ]; then
        echo "[fq-cache] MySQL 8.0 tarball cache hit: $dest"
        return 0
    fi

    local src full_name="mysql-8.0.45-linux-glibc2.28-x86_64.tar.xz"
    for src in \
        "${FQ_CI_MYSQL80_TARBALL:-}" \
        "${SOURCEDIR}/${tarball_name}" \
        "${SOURCEDIR}/${minimal_name}" \
        "${SOURCEDIR}/${full_name}" \
        "/data0/compat-packages/${tarball_name}" \
        "/data0/compat-packages/${minimal_name}" \
        "/data0/compat-packages/${full_name}"
    do
        if [ -n "$src" ] && [ -s "$src" ]; then
            echo "[fq-cache] copy prepared MySQL 8.0 tarball: $src -> $dest"
            cp -f "$src" "$dest" && return 0
        fi
    done

    for src in "${SOURCEDIR}"/mysql-*.tar.xz /data0/compat-packages/mysql-*.tar.xz; do
        if [ -s "$src" ]; then
            echo "[fq-cache] copy prepared MySQL 8.0 tarball: $src -> $dest"
            cp -f "$src" "$dest" && return 0
        fi
    done

    if ! command -v curl >/dev/null 2>&1; then
        echo "[fq-cache] WARN: curl not found; MySQL 8.0 cache will be prepared inside container"
        return 1
    fi

    local tmp="${dest}.tmp.$$"
    rm -f "$tmp"
    echo "[fq-cache] downloading MySQL 8.0 minimal tarball into host cache ..."
    if curl -fL \
            --retry 3 --retry-delay 5 --retry-connrefused \
            --connect-timeout 30 \
            --max-time "${FQ_CI_EXT_CACHE_DOWNLOAD_MAX_TIME:-900}" \
            -o "$tmp" "$url"; then
        if [ -s "$tmp" ]; then
            mv -f "$tmp" "$dest"
            return 0
        fi
    fi

    rm -f "$tmp"
    echo "[fq-cache] WARN: failed to prepare MySQL 8.0 host cache; test will fall back to container download"
    return 1
}

function _stage_fq_tarball_from_compat() {
    local shared_cache_dir="$1" thread_cache_dir="$2" dest_name="$3"
    shift 3
    local _src _pattern _dest="${shared_cache_dir}/${dest_name}"

    mkdir -p "$shared_cache_dir" "$thread_cache_dir" || return 1
    if [ -s "$_dest" ]; then
        echo "[fq-cache] ${dest_name} cache hit: $_dest"
    else
        for _src in "$@"; do
            if [ -n "$_src" ] && [ -s "$_src" ]; then
                echo "[fq-cache] copy prepared ${dest_name}: $_src -> $_dest"
                cp -f "$_src" "$_dest" && break
            fi
        done
    fi

    if [ -s "$_dest" ]; then
        echo "[fq-cache] stage ${dest_name} for thread ${thread_no}: ${thread_cache_dir}/${dest_name}"
        cp -f "$_dest" "${thread_cache_dir}/${dest_name}" \
            || echo "[fq-cache] WARN: failed to copy ${dest_name} into thread cache"
        return 0
    fi
    return 1
}

function _stage_fq_ext_env_cache() {
    local thread_cache_dir="$1"
    local shared_cache_dir="${FQ_CI_EXT_CACHE_DIR:-${CI_CACHE_DIR:-${WORKDIR}/cache}/fq-ext-env}"
    local mysql_name="fq-mysql-8.0.tar.xz"
    local influx_name="fq-influxdb-3.0.tar.gz"
    local influx_upstream="influxdb3-core-3.0.3_linux_amd64.tar.gz"

    mkdir -p "$thread_cache_dir"
    if _prepare_fq_mysql80_shared_cache "$shared_cache_dir"; then
        _stage_fq_tarball_from_compat "$shared_cache_dir" "$thread_cache_dir" "$mysql_name" \
            || echo "[fq-cache] WARN: failed to stage MySQL 8.0 tarball into thread cache"
    fi

    if ! _stage_fq_tarball_from_compat "$shared_cache_dir" "$thread_cache_dir" "$influx_name" \
            "${SOURCEDIR}/${influx_name}" \
            "${SOURCEDIR}/${influx_upstream}" \
            "/data0/compat-packages/${influx_name}" \
            "/data0/compat-packages/${influx_upstream}"; then
        local _influx_src
        for _influx_src in "${SOURCEDIR}"/influxdb3-core-*.tar.gz \
                           /data0/compat-packages/influxdb3-core-*.tar.gz; do
            if [ -s "$_influx_src" ]; then
                _stage_fq_tarball_from_compat "$shared_cache_dir" "$thread_cache_dir" "$influx_name" \
                    "$_influx_src" && break
            fi
        done
    fi

    local _apt_src
    for _apt_src in "${SOURCEDIR}"/fq-apt-pg16-*.tar.gz "${SOURCEDIR}"/fq-apt-postgis-pg16-*.tar.gz \
                    /data0/compat-packages/fq-apt-pg16-*.tar.gz \
                    /data0/compat-packages/fq-apt-postgis-pg16-*.tar.gz; do
        [ -s "$_apt_src" ] || continue
        _stage_fq_tarball_from_compat "$shared_cache_dir" "$thread_cache_dir" \
            "$(basename "$_apt_src")" "$_apt_src" || true
    done
}

TEST_VOLUME="${TMP_DIR}/thread_volume/${thread_no}"
TEST_VOLUME_READY="${TEST_VOLUME}/.ci-test-tree-ready"
mkdir -p "${TEST_VOLUME}/sim/var_taoslog"
mkdir -p "${TEST_VOLUME}/sim/tsim"
mkdir -p "${TEST_VOLUME}/coredump"
rm -rf "${TEST_VOLUME}/coredump"/*

# The volume is mounted directly as the container's test directory.  Copy the
# tree once per slot and publish the marker only after the required entrypoints
# are present; a failed copy therefore retries on the next case.
if [ ! -f "${TEST_VOLUME_READY}" ] || \
   [ ! -f "${TEST_VOLUME}/ci/run_case.sh" ] || \
   [ ! -d "${TEST_VOLUME}/cases" ] || \
   [ ! -f "${TEST_VOLUME}/requirements.txt" ]; then
    rm -f "${TEST_VOLUME_READY}"
    echo "[ci] preparing test tree for slot ${thread_no}"
    if ! cp -rf "${REPDIR}/test/"* "${TEST_VOLUME}/"; then
        echo "ERROR: failed to prepare test tree in ${TEST_VOLUME}" >&2
        exit 1
    fi
    if [ ! -f "${TEST_VOLUME}/ci/run_case.sh" ] || \
       [ ! -d "${TEST_VOLUME}/cases" ] || \
       [ ! -f "${TEST_VOLUME}/requirements.txt" ]; then
        echo "ERROR: incomplete test tree in ${TEST_VOLUME}" >&2
        exit 1
    fi
    : > "${TEST_VOLUME_READY}"
fi

# if [ ! -f "${SOURCEDIR}/${packageName}" ]; then
#      wget -P  ${SOURCEDIR} https://taosdata.com/assets-download/3.0/${packageName}
# fi

# MOUNT_DIR="$TMP_DIR/thread_volume/$thread_no/$exec_dir:$CONTAINER_TESTDIR/test/$exec_dir"
MOUNT_SOURCE="${TMP_DIR}/thread_volume/${thread_no}"
MOUNT_DIR="${MOUNT_SOURCE}:${CONTAINER_TESTDIR}/test"
echo "$thread_no -> ${exec_dir}:$cmd"
coredump_dir=$(cat /proc/sys/kernel/core_pattern | xargs dirname)
if [ -z "$coredump_dir" ] || [ "$coredump_dir" = "." ]; then
    coredump_dir="/home/coredump"
fi

name_param=""
[ -n "$container_name" ] && name_param="--name ${container_name}"

# san=y: 注入 CI_ASAN_BUILD=1，pytest.sh 会 LD_PRELOAD libasan.so.8 启用 ASAN 检测。
# san=n: 完全不做 ASAN，设 CI_NO_ASAN=1
asan_env="-e CI_NO_ASAN=1"
if [[ "${buildSan}" == "y" ]]; then
  asan_env="-e CI_NO_ASAN=1 -e CI_ASAN_BUILD=1"
fi

# 连接器相关参数用数组拼装并整体引用展开：路径或键值对含空格时，未加引号的 `:+`
# 展开会被 bash 按空白拆分成错位的 -v/-e 参数。展开处统一写 ${arr[@]+"${arr[@]}"}：
# bash 4.3 及更早版本对空数组的 "${arr[@]}" 会展开出一个空参数（被 docker run 当成
# 镜像名而失败），4.4 起才展开为零个参数；该写法在两个版本段行为一致。追加用显式
# if 而非 "[ -n ... ] &&" 尾接形式，避免脚本被 source 且调用方启用 set -e 时条件为
# 假导致整体退出码 1 中止。echo 预览行与真实 docker run 使用同一展开，保证打印的
# 命令与实际执行一致；预览按空格拼接展示数组元素，仅供人工阅读（含空格路径时不可
# 直接复制重跑）。REP_MOUNT_INTERNAL/REP_MOUNT_DOCS 是历史既有写法，不在本次改动
# 范围内。
# 自检：docker run 的 connector_args 展开必须保留 ${connector_args[@]+"${connector_args[@]}"}
# 防护写法（bash<4.4 空数组直接 "${arr[@]}" 会注入空参数、被 docker 当镜像名，见文件
# 头兼容性约束）。语义级 smoke 校验无法覆盖使用点，改为源码级断言：一旦下方任一使用
# 处被「简化」，本检查立即失败并指出原因。锚点用 BASH_SOURCE（$0 在 bash -c/source/
# 管道执行下不指向脚本文件）并做 -f 判断：非常规文件（进程替换/stdin）时跳过自检，
# 不让防御性检查本身阻断 CI
_self_src="${BASH_SOURCE[0]:-}"
if [ -f "${_self_src}" ]; then
    # 反向断言：禁止「不带 + 前缀守卫的裸 ${connector_args[@]} 展开」出现在任何行。
    # 不能正向 grep 守卫写法本身——模式字面串会匹配到注释/grep 自己（自检恒真）；
    # 也不能让裸串出现在本自检里，故模式用转义正则书写（\$\{ 不构成字面量 ${ ），
    # 且排除前导 + 的合法内嵌形态
    _idiom_pat='(^|[^+])"\$\{connector_args\[@\]\}"'
    # 扫描前剥离整行注释：注释里出现裸展开「反例」文本不应触发守卫（自检自身/头部注释
    # 都在讨论该写法）；代码行中的合法守卫形态因前置 + 而不匹配。只剥「# 位于行首或
    # 前面仅有空白」的整行注释：若按任意 # 截断，代码/字符串中行内 #（如 ${VAR#pattern}
    # 参数展开）之后的正文会被误删，导致漏检
    # command -v guards: without sed/grep (busybox-less images) the pipeline
    # yields an empty stream and a failing grep, which the if below would read
    # as "no violation" -- a silent skip this check never promised.
    if ! command -v sed >/dev/null 2>&1 || ! command -v grep >/dev/null 2>&1; then
        echo "[run_container] WARN: sed/grep missing; cannot run the connector_args guard self-check (a bare expansion would go undetected)" >&2
    elif sed -e 's/^[[:space:]]*#.*$//' "${_self_src}" | grep -qE "${_idiom_pat}"; then
        echo "[run_container] ERROR: a bare (un-guarded) connector_args expansion was found; every usage must keep the bash<4.4-safe form with the + guard (see header)" >&2
        exit 1
    elif [ "${PIPESTATUS[1]:-0}" -gt 1 ]; then
        # grep exit 1 means "no match" -- the expected pass. Only >=2 is a real
        # tool failure worth an "unchecked" warning.
        echo "[run_container] WARN: connector_args guard self-check pipeline errored (grep rc=${PIPESTATUS[1]}); result is unchecked (a bare expansion would go undetected)" >&2
    fi
else
    echo "[run_container] WARN: cannot stat own source '${_self_src}'; skipping the connector_args guard self-check" >&2
fi
unset _self_src _idiom_pat
connector_args=()
if [ -n "$REP_MOUNT_CONNECTOR" ]; then
    connector_args+=(-v "$REP_MOUNT_CONNECTOR")
fi
if [ -n "$CONNECTOR_PY_ENV_ARG" ]; then
    connector_args+=(-e "$CONNECTOR_PY_ENV_ARG")
fi

# 挂载路径含空格时，下方多个按历史写法不加引号展开的 -v 变量（REP_MOUNT_PARAM/
# REP_MOUNT_DEBUG/REP_MOUNT_LIB/$MOUNT_DIR/$SOURCEDIR/$TMP_DIR 等）会被空白拆分，
# docker 会把错误字段当镜像名报出难定位的错（invalid reference format）。与其
# 事后从 docker 报错反推，不如前置断言直接失败并指明是哪个路径。当前 CI
# runner 的路径均不含空格，此断言不改变现状；case 模式为 POSIX 兼容（bash 4.2）。
# 循环内变量统一 ${var:-} 展开：当前各布局分支都会赋值（REP_MOUNT_INTERNAL/DOCS
# 仅 tsdb 分支、其余三处布局均有），但断言自身的契约就是「set -u 调用方不因未定义
# 变量先于诊断中止」（与文件其它位置 ${REP_MOUNT_INTERNAL:+...} 的惯例一致），且能
# 免疫未来新增布局分支漏赋值的情况。
# 维护约定（契约范围：仅「不加引号展开」的挂载路径类变量——它们源自 checkout 路径，
# 是空格的现实载体；asan_env/name_param/extra_param/exec_dir/cmd 等脚本内构造的参数
# token 不在契约内；REP_MOUNT_CONNECTOR/CONNECTOR_PY_ENV_ARG 经 connector_args 带引号安全
# 展开、含空格本可用，故意不入列以免误杀）：下方 docker run 里新增/改为不加引号展开
# 的挂载路径类变量，必须同步加入本列表——漏项会把失败形态退化回难定位的 docker
# invalid reference format。注意本检查是按「变量」而非「展开结果」划分边界：由多个子串
# 拼接的不加引号挂载表达式（如 ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir）
# 要求每个路径组件都在列（TMP_DIR/coredump_dir 已入列，thread_no 为数字循环索引无空格），
# 新增同类拼接表达式时同样逐组件核对。本列表是尽力而为的启发式前置断言，通过断言不代表
# 下方所有不加引号展开在任意输入下都安全——它针对的现实风险是 checkout 路径含空格。
for _mount_path in "${REP_MOUNT_PARAM:-}" "${REP_MOUNT_INTERNAL:-}" "${REP_MOUNT_DOCS:-}" \
                   "${REP_MOUNT_DEBUG:-}" "${REP_MOUNT_LIB:-}" "${MOUNT_DIR:-}" "${SOURCEDIR:-}" "${TMP_DIR:-}" \
                   "${coredump_dir:-}" "${CONTAINER_TESTDIR:-}" "${_parent_docs:-}"; do
    case "$_mount_path" in
        *" "*)
            echo "[run_container] ERROR: mount path contains spaces (unsupported by the unquoted -v expansions below): '${_mount_path}'" >&2
            exit 1
            ;;
    esac
done
unset _mount_path
# TSDB FQ tasks provision one exclusive database lease on the runner. Keep an
# explicit local-mode escape hatch while rolling out to older worker hosts.
# Coverage CI checks out TDinternal/community only, so the monorepo-relative
# runner is absent there. Look up the same run-ci.py the tsdb pipeline uses.
_fq_ci_resolve_runner() {
    if [[ -n "${FQ_CI_RUNNER:-}" && -f "${FQ_CI_RUNNER}" ]]; then
        echo "${FQ_CI_RUNNER}"
        return 0
    fi
    local community_root candidate
    community_root="$(cd "$REPDIR" && pwd -P)"
    candidate="$(dirname "$(dirname "$community_root")")/tools/cicd/tsdb-test-pipeline/fq/run-ci.py"
    if [[ -f "$candidate" ]]; then
        echo "$candidate"
        return 0
    fi
    if [[ -n "${FQ_CI_TOOLS_DIR:-}" && -f "${FQ_CI_TOOLS_DIR}/fq/run-ci.py" ]]; then
        echo "${FQ_CI_TOOLS_DIR}/fq/run-ci.py"
        return 0
    fi
    candidate="/root/tsdb/tools/cicd/tsdb-test-pipeline/fq/run-ci.py"
    if [[ -f "$candidate" ]]; then
        echo "$candidate"
        return 0
    fi
    return 1
}

fq_runner=()
fq_default_services=()
if fq_ci_needs_databases "$cmd" && [[ "${FQ_CI_CONTAINER_ENV:-1}" == "1" ]]; then
    if [[ -n "${TSDB_COMMUNITY:-}" ]]; then
        FQ_COMMUNITY="$(cd "$REPDIR" && pwd -P)"
        FQ_RUNNER="$(dirname "$(dirname "$FQ_COMMUNITY")")/tools/cicd/tsdb-test-pipeline/fq/run-ci.py"
        if [[ ! -f "$FQ_RUNNER" ]]; then
            echo "ERROR: FQ container runner missing: $FQ_RUNNER" >&2
            exit 1
        fi
    else
        FQ_RUNNER="$(_fq_ci_resolve_runner)" || {
            echo "ERROR: FQ container runner not found. Set FQ_CI_RUNNER to tools/cicd/tsdb-test-pipeline/fq/run-ci.py" >&2
            exit 1
        }
        # Tasks that omit FQ_REQUIRED_SERVICES still talk to MySQL, PostgreSQL,
        # Influx and SQL Server. Leave Oracle off unless the task asks for it,
        # so a coverage worker is not required to hold the Oracle image.
        if [[ "$cmd" != *FQ_REQUIRED_SERVICES=* ]]; then
            fq_default_services=(-e FQ_REQUIRED_SERVICES=mysql,pg,influx,sqlserver)
        fi
        export FQ_CI_PLACEMENT="${FQ_CI_PLACEMENT:-local-first}"
        export FQ_CI_LOCAL_MAX_SLOTS="${FQ_CI_LOCAL_MAX_SLOTS:-auto}"
        export FQ_CI_PREPARE_TIMEOUT="${FQ_CI_PREPARE_TIMEOUT:-1200}"
        export FQ_CI_AIO_MAX_NR="${FQ_CI_AIO_MAX_NR:-4194304}"
    fi
    echo "[FQ CI] lease runner: $FQ_RUNNER"
    fq_runner=(python3 "$FQ_RUNNER" --artifacts "$MOUNT_SOURCE/fq-artifacts/attempt-${FQ_CI_ATTEMPT:-1}"
               --cert-dir "$REPDIR/test/cases/09-DataQuerying/19-FederatedQuery/certs" --)
    export FQ_CI_HYBRID="${FQ_CI_HYBRID:-1}"
fi

mock_env=""
if [[ "${CHILD_PIPELINE_NAME:-}" == "tsdb-ci-linux" && "${cmd}" =~ (cases/[^[:space:]]+\.py) ]]; then
  case_file="${REPDIR}/test/${BASH_REMATCH[1]}"
  if [[ -f "${case_file}" ]]; then
    mock_mode=$(sed -n 's/^XNODED_CI_MOCK_MODE = "\(syntax\|priv\)"$/\1/p' "${case_file}")
    case "${mock_mode}" in
      syntax|priv) mock_env="-e XNODED_TEST_MOCK_MODE=${mock_mode}" ;;
    esac
  fi
fi

# Dynamic workers verify and remove containers themselves. Keep --rm for
# standalone callers, which do not have a worker cleanup owner.
_ci_remove_flag=--rm
[[ "${CI_WORKER_MANAGED_CONTAINER:-0}" != 1 ]] || _ci_remove_flag=""

echo "docker run \
    ${fq_default_services[@]+"${fq_default_services[@]} "} \
    ${name_param:+$name_param }--privileged=true \
    $asan_env \
    $mock_env \
    -e CI_MIN_RESERVED_MEMORY_MB=${CI_MIN_RESERVED_MEMORY_MB:-2048} \
    -v $REP_MOUNT_PARAM \
    ${REP_MOUNT_INTERNAL:+-v $REP_MOUNT_INTERNAL} \
    ${connector_args[@]+"${connector_args[@]}"} \
    ${REP_MOUNT_DOCS:+-v $REP_MOUNT_DOCS} \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v \"$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}\" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    ${_ci_remove_flag} --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3} $CONTAINER_TESTDIR/test/ci/run_case.sh -d \"$exec_dir\" -c \"$cmd\" $extra_param"
_ci_exit_marker="${TMP_DIR}/thread_volume/${thread_no}/.ci-container-exit"
rm -f "${_ci_exit_marker}" "${TMP_DIR}/thread_volume/${thread_no}/.ci-test-command-exit"
_ci_docker_start_ms=$(date +%s%3N)
echo "[ci-timing] stage=docker-run event=start"
"${fq_runner[@]}" docker run \
    ${fq_default_services[@]+"${fq_default_services[@]}"} \
    ${name_param:+$name_param} --privileged=true \
    $asan_env \
    $mock_env \
    -e CI_CONTAINER_EXIT_MARKER="$CONTAINER_TESTDIR/test/.ci-container-exit" \
    -e CI_TEST_COMMAND_EXIT_MARKER="$CONTAINER_TESTDIR/test/.ci-test-command-exit" \
    -e "CI_MIN_RESERVED_MEMORY_MB=${CI_MIN_RESERVED_MEMORY_MB:-2048}" \
    -v $REP_MOUNT_PARAM \
    ${REP_MOUNT_INTERNAL:+-v $REP_MOUNT_INTERNAL} \
    ${connector_args[@]+"${connector_args[@]}"} \
    ${REP_MOUNT_DOCS:+-v $REP_MOUNT_DOCS} \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    ${_ci_remove_flag} --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3} $CONTAINER_TESTDIR/test/ci/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
_ci_docker_end_ms=$(date +%s%3N)
echo "[ci-timing] stage=docker-run elapsed_ms=$(( _ci_docker_end_ms - _ci_docker_start_ms )) rc=${ret}"
if [[ -f "${_ci_exit_marker}" ]]; then
    read -r _ci_entry_exit_ms _ci_entry_rc < "${_ci_exit_marker}"
    if [[ "${_ci_entry_exit_ms}" =~ ^[0-9]+$ ]]; then
        echo "[ci-timing] stage=container-removal elapsed_ms=$(( _ci_docker_end_ms - _ci_entry_exit_ms )) entrypoint_rc=${_ci_entry_rc}"
    fi
else
    echo "[ci-timing] stage=container-removal unavailable (entrypoint exit not observed)"
fi
exit $ret
