#!/bin/bash

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
    else
        REPDIR=$WORKDIR/TDengine
        REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
        CONTAINER_TESTDIR=/home/TDengine
        SIM_DIR=/home/TDengine/sim
        REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
        REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
        REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDengine/debug/build/lib:ro"
    fi
else
    # ── tsdb CI 布局 ────────────────────────────────────────────────────
    # tsdb 仓库原生路径：脚本自身位于 source/taos-community/test/ci/run_container.sh
    # 优先从脚本位置反推，兼容 sparse-checkout 创建的 symlink
    [ $ent -ne 0 ] && extra_param="$extra_param -e"
    SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
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
SOURCEDIR=$WORKDIR/src
MOUNT_DIR=""
# packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
# 若宿主机预置了兼容性测试安装包缓存目录，则将 SOURCEDIR 指向它，
# 避免测试时从公网重复下载（兼容 large-mem runner 预置包场景）。
if [ -d "/data0/compat-packages" ]; then
    ln -sfn /data0/compat-packages "$SOURCEDIR"
else
    mkdir -p "$SOURCEDIR"
fi

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

mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/var_taoslog
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/test" ]; then
    if [ "$exec_dir" != "." ]; then
        subdir=$(echo "$exec_dir"|cut -d/ -f1)
        echo "cp -rf ${REPDIR}/test/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
        cp -rf ${REPDIR}/test/$subdir ${TMP_DIR}/thread_volume/$thread_no/
    else
        echo "cp -rf ${REPDIR}/test/* ${TMP_DIR}/thread_volume/$thread_no/"
        cp -rf "${REPDIR}/test/"* "${TMP_DIR}/thread_volume/$thread_no/"
    fi
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

echo "docker run \
    ${name_param:+$name_param }--privileged=true \
    $asan_env \
    -v $REP_MOUNT_PARAM \
    ${REP_MOUNT_INTERNAL:+-v $REP_MOUNT_INTERNAL} \
    ${REP_MOUNT_DOCS:+-v $REP_MOUNT_DOCS} \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v \"$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}\" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    --rm --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3} $CONTAINER_TESTDIR/test/ci/run_case.sh -d \"$exec_dir\" -c \"$cmd\" $extra_param"
docker run \
    ${name_param:+$name_param} --privileged=true \
    $asan_env \
    -v $REP_MOUNT_PARAM \
    ${REP_MOUNT_INTERNAL:+-v $REP_MOUNT_INTERNAL} \
    ${REP_MOUNT_DOCS:+-v $REP_MOUNT_DOCS} \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    --rm --ulimit core=-1 ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3} $CONTAINER_TESTDIR/test/ci/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
exit $ret
