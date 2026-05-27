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
    # ── tsdb CI 布局优先：从脚本自身位置反推 source/taos-community ────────────
    # 本脚本位于 source/taos-community/tests/parallel_test/run_container.sh
    # realpath 解析软链接（$0 可能经由 TDinternal/community 软链调用），
    # 得到 CI_PROJECT_DIR/source/taos-community 的物理路径
    _SCRIPT_REAL="$(realpath "$0")"
    _TC_DIR="$(realpath "$(dirname "${_SCRIPT_REAL}")/../..")"  # → source/taos-community
    if [ -d "${_TC_DIR}/source" ] && [ -d "${_TC_DIR}/tests" ]; then
        # tsdb CI 布局：直接使用 source/taos-community 物理路径
        REPDIR="${_TC_DIR}"
    elif [ -d "$WORKDIR/TDinternal/community" ]; then
        REPDIR="$(realpath "$WORKDIR/TDinternal/community")"
    else
        echo "ERROR: Cannot find source/taos-community under $WORKDIR or script dir"
        exit 1
    fi
    echo "[run_container] REPDIR=${REPDIR}"
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/mnt/tsdb/source/taos-community"
    SIM_DIR="/mnt/tsdb/sim"
    REP_MOUNT_PARAM="${REPDIR}:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
else
    # ── tsdb CI 布局优先（与 ent 分支逻辑相同）────────────────────────────────
    _SCRIPT_REAL="$(realpath "$0")"
    _TC_DIR="$(realpath "$(dirname "${_SCRIPT_REAL}")/../..")"  # → source/taos-community
    if [ -d "${_TC_DIR}/source" ] && [ -d "${_TC_DIR}/tests" ]; then
        # tsdb CI 布局：source/taos-community 物理路径
        REPDIR="${_TC_DIR}"
    elif [ -d "$WORKDIR/TDinternal/community" ]; then
        REPDIR="$(realpath "$WORKDIR/TDinternal/community")"
    else
        # 旧版 TDengine 单仓布局 fallback
        REPDIR="$WORKDIR/TDengine"
    fi
    echo "[run_container] REPDIR=${REPDIR}"
    REPDIR_DEBUG="$WORKDIR/$DEBUGPATH/"
    CONTAINER_TESTDIR="/mnt/tsdb/source/taos-community"
    SIM_DIR="/mnt/tsdb/sim"
    REP_MOUNT_PARAM="$REPDIR:/mnt/tsdb/source/taos-community"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/mnt/tsdb/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/mnt/tsdb/debug/build/lib:ro"
fi

# 设置临时目录

ulimit -c unlimited
TMP_DIR="$WORKDIR/tmp"
SOURCEDIR="$WORKDIR/src"
MOUNT_DIR=""
# packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
rm -rf "${TMP_DIR}/thread_volume/$thread_no/sim"
# 若宿主机预置了兼容性测试安装包缓存目录，则将 SOURCEDIR 指向它，
# 避免测试时从公网重复下载（兼容 large-mem runner 预置包场景）。
if [ -d "/data0/compat-packages" ]; then
    ln -sfn /data0/compat-packages "$SOURCEDIR"
else
    mkdir -p "$SOURCEDIR"
fi
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
_core_pat=$(cat /proc/sys/kernel/core_pattern)
coredump_dir=""
if [[ "$_core_pat" != \|* ]]; then
    coredump_dir=$(dirname "$_core_pat" 2>/dev/null) || true
fi
if [ -z "$coredump_dir" ] || [ "$coredump_dir" = "." ]; then
    coredump_dir="/corefile"
fi

SIM_VOL="$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}"
CORE_VOL="$TMP_DIR/thread_volume/$thread_no/coredump:${coredump_dir}"
TAOSLOG_VOL="$TMP_DIR/thread_volume/$thread_no/taoslog:/var/log/taos"

docker_cmd=(
    docker run --privileged=true
    -e CI_NO_ASAN=1
    -v "${REP_MOUNT_PARAM}"
    -v "${REP_MOUNT_DEBUG}"
    -v "${REP_MOUNT_LIB}"
    -v "${MOUNT_DIR}"
    -v "${SOURCEDIR}:/usr/local/src/"
    -v "${SIM_VOL}"
    -v "${CORE_VOL}"
    -v "${TAOSLOG_VOL}"
    --rm --ulimit core=-1
    "${DOCKER_IMAGE_NAME:-tdengine-ci:0.3}"
    "$CONTAINER_TESTDIR/tests/parallel_test/run_case.sh"
    -d "${exec_dir}"
    -c "${cmd}"
)

# debugSan 构建已通过 -DBUILD_SANITIZER=1 将 ASAN 编译进二进制（GCC14, libasan.so.8）。
# CI_ASAN_BUILD=1 告知 pytest.sh 通过 LD_PRELOAD 注入 libasan.so.8（匹配 GCC14 构建）。
if [ "${buildSan}" == "y" ]; then
    docker_cmd=("${docker_cmd[@]:0:2}" -e CI_ASAN_BUILD=1 "${docker_cmd[@]:2}")
fi

# 容器命名参数（用于 cancel 时按名字 stop）
if [ -n "${container_name:-}" ]; then
    docker_cmd=("${docker_cmd[@]:0:2}" --name "${container_name}" "${docker_cmd[@]:2}")
fi

if [[ -n "${extra_param:-}" ]]; then
    read -r -a _extra_args <<< "${extra_param}"
    docker_cmd+=("${_extra_args[@]}")
fi

printf '%q ' "${docker_cmd[@]}"
echo
"${docker_cmd[@]}"

ret=$?
exit "$ret"