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
container_name=""
while getopts "w:d:c:t:n:o:s:eh" opt; do
    case $opt in
        w) WORKDIR=$OPTARG ;;
        d) exec_dir=$OPTARG ;;
        c) cmd=$OPTARG ;;
        t) thread_no=$OPTARG ;;
        n) container_name=$OPTARG ;;
        e) ent=1 ;;
        o) extra_param="-o $OPTARG" ;;
        s) buildSan=$OPTARG ;;
        h) usage; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG"; usage; exit 1 ;;
    esac
done

# 检查必需的变量
if [ -z "$WORKDIR" ] || [ -z "$exec_dir" ] || [ -z "$cmd" ] || [ -z "$thread_no" ]; then
    usage
    exit 1
fi

# 设置编译环境
if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
elif [[ "${buildSan}" == "n" ]] || [[ -z "${buildSan}" ]]; then
    DEBUGPATH="debugNoSan"
else
    usage
    exit 1
fi

# 设置目录和挂载参数
if [ $ent -ne 0 ]; then
    extra_param="$extra_param -e"
    INTERNAL_REPDIR="$WORKDIR/TDinternal"
    REPDIR="$(realpath ${INTERNAL_REPDIR}/community)"
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/home/TDinternal/community"
    SIM_DIR="/home/TDinternal/sim"
    REP_MOUNT_PARAM="${REPDIR}:/home/TDinternal/community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
else
    REPDIR="$WORKDIR/TDengine"
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/home/TDinternal/community"
    SIM_DIR="/home/TDinternal/sim"
    REP_MOUNT_PARAM="$REPDIR:/home/TDinternal/community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
fi

# 设置临时目录

ulimit -c unlimited
TMP_DIR="$WORKDIR/tmp"
SOURCEDIR="$WORKDIR/src"
MOUNT_DIR=""
# packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
rm -rf "${TMP_DIR}/thread_volume/$thread_no/sim"
mkdir -p "$SOURCEDIR"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/sim/var_taoslog"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/sim/tsim"
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/coredump"
rm -rf "${TMP_DIR}/thread_volume/$thread_no/coredump"/*
# taoslog 实时挂载到宿主机，即使容器被强制 kill 日志也不会丢失
mkdir -p "${TMP_DIR}/thread_volume/$thread_no/taoslog"
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
TAOSLOG_VOL="$TMP_DIR/thread_volume/$thread_no/taoslog:/var/log/taos"

# 容器命名参数（用于 cancel 时按名字 stop）
name_param=""
[ -n "$container_name" ] && name_param="--name \"$container_name\""

# debugSan 构建已通过 -DBUILD_SANITIZER=1 将 ASAN 编译进二进制（GCC14, libasan.so.8）。
# CI_ASAN_BUILD=1 告知 pytest.sh 通过 LD_PRELOAD 注入 libasan.so.8（匹配 GCC14 构建）。
# CI_NO_ASAN=1 同时设置，禁止旧式 LD_PRELOAD 逻辑（两者兼容，pytest.sh 优先检查 CI_ASAN_BUILD）。
if [ "${buildSan}" == "y" ]; then
    asan_env="-e CI_NO_ASAN=1 -e CI_ASAN_BUILD=1"
else
    asan_env="-e CI_NO_ASAN=1"
fi

docker_cmd="docker run --privileged=true \$asan_env \
    $name_param \
    -v \"${REP_MOUNT_PARAM}\" \
    -v \"${REP_MOUNT_DEBUG}\" \
    -v \"${REP_MOUNT_LIB}\" \
    -v \"${MOUNT_DIR}\" \
    -v \"${SOURCEDIR}:/usr/local/src/\" \
    -v \"${SIM_VOL}\" \
    -v \"${CORE_VOL}\" \
    -v \"${TAOSLOG_VOL}\" \
    --rm --ulimit core=-1 tdengine-ci:0.1 $CONTAINER_TESTDIR/tests/parallel_test/run_case.sh -d ${exec_dir} -c \"${cmd}\" ${extra_param}"

echo "$docker_cmd"  
eval "$docker_cmd"

ret=$?
exit "$ret"