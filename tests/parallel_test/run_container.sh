#!/bin/bash
set -e  # 确保任何命令失败时退出

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
while getopts "w:d:c:t:o:s:eh" opt; do
    case $opt in
        w) WORKDIR=$OPTARG ;;
        d) exec_dir=$OPTARG ;;
        c) cmd=$OPTARG ;;
        t) thread_no=$OPTARG ;;
        e) ent=1 ;;
        o) extra_param="-o $OPTARG" ;;
        s) buildSan=$OPTARG ;;
        h) usage; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG"; usage; exit 1 ;;
    esac
done

if [ -z "$WORKDIR" ] || [ -z "$exec_dir" ] || [ -z "$cmd" ] || [ -z "$thread_no" ]; then
    usage
    exit 1
fi

if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
elif [[ "${buildSan}" == "n" ]] || [[ -z "${buildSan}" ]]; then
    DEBUGPATH="debugNoSan"
else
    usage
    exit 1
fi

if [ -d "$WORKDIR/TDinternal/enterprise" ]; then
    # ── TDinternal CI 布局 ──────────────────────────────────────────────
    if [ $ent -ne 0 ]; then
        echo "TDinternal-Enterprise edition selected"
        extra_param="$extra_param -e"
        INTERNAL_REPDIR="$WORKDIR/TDinternal"
        REPDIR="$INTERNAL_REPDIR/community"
        REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
        CONTAINER_TESTDIR="/home/TDinternal/community"
        SIM_DIR="/home/TDinternal/sim"
        REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"
        REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
        REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
    else
        echo "TDinternal-OSS edition selected"
        REPDIR="$WORKDIR/TDengine"
        REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
        CONTAINER_TESTDIR="/home/TDengine"
        SIM_DIR="/home/TDengine/sim"
        REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
        REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
        REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDengine/debug/build/lib:ro"
    fi
else
    # ── tsdb CI 布局 ────────────────────────────────────────────────────
    # tsdb 仓库原生路径：脚本位于 tests/parallel_test/run_container.sh
    # 优先从脚本位置反推，兼容 sparse-checkout 创建的 symlink
    [ $ent -ne 0 ] && extra_param="$extra_param -e"
    echo "TSDB edition selected (ent=$ent)"
    SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
    TSDB_COMMUNITY=$(cd "${SCRIPT_DIR}/../.." && pwd)  # → source/taos-community
    if [ -d "${TSDB_COMMUNITY}/tests" ] && [ -d "${TSDB_COMMUNITY}/source" ]; then
        REPDIR="${TSDB_COMMUNITY}"
    elif [ -d "$WORKDIR/TDengine" ]; then
        REPDIR="$WORKDIR/TDengine"
    elif [ -d "$WORKDIR/TDinternal/community" ]; then
        REPDIR="$WORKDIR/TDinternal/community"
    else
        echo "ERROR: Cannot find source directory under $WORKDIR"
        exit 1
    fi
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/mnt/tsdb/source/taos-community"
    SIM_DIR="/mnt/tsdb/sim"
    REP_MOUNT_PARAM="${REPDIR}:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
fi

ulimit -c unlimited
TMP_DIR="$WORKDIR/tmp"
SOURCEDIR="$WORKDIR/src"
MOUNT_DIR=""
rm -rf "${TMP_DIR}/thread_volume/$thread_no/sim"
mkdir -p "$SOURCEDIR"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/sim/var_taoslog"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/sim/tsim"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/coredump"
rm -rf "${TMP_DIR}/thread_volume/$thread_no/coredump"/*
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/$exec_dir" ]; then
    subdir=$(echo "$exec_dir"|cut -d/ -f1)
    echo "cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
    cp -rf "${REPDIR}/tests/$subdir" "${TMP_DIR}/thread_volume/$thread_no/"
fi
MOUNT_DIR="$TMP_DIR/thread_volume/$thread_no/$exec_dir:$CONTAINER_TESTDIR/tests/$exec_dir"

echo "$thread_no -> ${exec_dir}:$cmd"
coredump_dir=$(cat /proc/sys/kernel/core_pattern | xargs dirname)
if [ -z "$coredump_dir" ] || [ "$coredump_dir" = "." ]; then
    coredump_dir="/home/coredump"
fi

SIM_VOL="$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}"
CORE_VOL="$TMP_DIR/thread_volume/$thread_no/coredump:/home/coredump"

docker_cmd="docker run --privileged=true \
    -v \"${REP_MOUNT_PARAM}\" \
    -v \"${REP_MOUNT_DEBUG}\" \
    -v \"${REP_MOUNT_LIB}\" \
    -v \"${MOUNT_DIR}\" \
    -v \"${SOURCEDIR}:/usr/local/src/\" \
    -v \"${SIM_VOL}\" \
    -v \"${CORE_VOL}\" \
    --rm --ulimit core=-1 tdengine-ci:0.1 $CONTAINER_TESTDIR/tests/parallel_test/run_case.sh -d ${exec_dir} -c \"${cmd}\" ${extra_param}"

echo "$docker_cmd"  
eval "$docker_cmd"

ret=$?
exit "$ret"