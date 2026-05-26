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

if [ $ent -ne 0 ]; then
    extra_param="$extra_param -e"
    INTERNAL_REPDIR=$WORKDIR/TDinternal
    REPDIR=$(realpath ${INTERNAL_REPDIR}/community)
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/mnt/tsdb/source/taos-community
    SIM_DIR=/mnt/tsdb/sim
    REP_MOUNT_PARAM="${REPDIR}:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
else
    # ── tsdb CI 布局优先：从脚本自身位置反推 source/taos-community ────────────
    _SCRIPT_REAL="$(realpath "$0")"
    _TC_DIR="$(realpath "$(dirname "${_SCRIPT_REAL}")/../..")"  # → source/taos-community
    if [ -d "${_TC_DIR}/source" ] && [ -d "${_TC_DIR}/tests" ]; then
        REPDIR="${_TC_DIR}"
    elif [ -d "$WORKDIR/TDinternal/community" ]; then
        REPDIR="$(realpath "$WORKDIR/TDinternal/community")"
    else
        REPDIR="$WORKDIR/TDengine"
    fi
    echo "[run_container] REPDIR=${REPDIR}"
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/mnt/tsdb/source/taos-community
    SIM_DIR=/mnt/tsdb/sim
    REP_MOUNT_PARAM="$REPDIR:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
fi

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
# docs overlay：TDinternal 布局下 docs/ 在仓库根而非 community/ 下，
# 补充挂载使 test_check_error_code.py 等用例可访问文档路径
REP_MOUNT_DOCS=""
if [ ! -d "${REPDIR}/docs" ]; then
    _parent_docs="$(dirname "$REPDIR")/docs"
    if [ -d "$_parent_docs" ]; then
        REP_MOUNT_DOCS="${_parent_docs}:${CONTAINER_TESTDIR}/docs"
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
MOUNT_TARGET="${CONTAINER_TESTDIR}/test"
MOUNT_DIR="${MOUNT_SOURCE}:${MOUNT_TARGET}"
echo "$thread_no -> ${exec_dir}:$cmd"
_core_pat=$(cat /proc/sys/kernel/core_pattern)
coredump_dir=""
if [[ "$_core_pat" != \|* ]]; then
    coredump_dir=$(dirname "$_core_pat" 2>/dev/null) || true
fi
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
    --rm --ulimit core=-1 tdengine-ci:0.1 $CONTAINER_TESTDIR/test/ci/run_case.sh -d \"$exec_dir\" -c \"$cmd\" $extra_param"
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
    --rm --ulimit core=-1 tdengine-ci:0.1 $CONTAINER_TESTDIR/test/ci/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
exit $ret
