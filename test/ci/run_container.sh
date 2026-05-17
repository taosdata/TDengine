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
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    --rm --ulimit core=-1 tdengine-ci:0.1 $CONTAINER_TESTDIR/test/ci/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
exit $ret
