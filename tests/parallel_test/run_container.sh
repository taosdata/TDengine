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

if [ $ent -ne 0 ]; then
    echo "TSDB-Enterprise edition selected"
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
    echo "TSDB-OSS edition selected"
    REPDIR="$WORKDIR/TDengine"
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/home/TDengine"
    SIM_DIR="/home/TDengine/sim"
    REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDengine/debug/build/lib:ro"
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