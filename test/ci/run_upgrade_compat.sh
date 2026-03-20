#!/bin/bash
# run_upgrade_compat.sh
#
# 冷/热升级兼容性测试启动脚本（宿主机侧）
#
# 功能：
#   1. 从 HTTP 服务器下载所需绿色版本到 /green_versions/（有缓存，存在则跳过）
#   2. 在独立 Docker 容器中运行冷/热升级兼容性测试
#   3. 输出测试结果日志
#
# 用法：
#   ./run_upgrade_compat.sh -w WORKDIR [-l LOG_DIR] [-e] [-h]
#
# 参数：
#   -w WORKDIR   工作目录（与 run.sh 相同，含 TDinternal/ 和 debugNoSan/）
#   -l LOG_DIR   日志输出目录（默认：WORKDIR/upgrade_compat_logs）
#   -e           企业版模式
#   -h           帮助

function usage() {
    echo "Usage: $0 -w WORKDIR [-l LOG_DIR] [-e] [-h]"
    echo ""
    echo "  -w WORKDIR   Working directory (contains TDinternal/ and debugNoSan/)"
    echo "  -l LOG_DIR   Log output directory (default: WORKDIR/upgrade_compat_logs)"
    echo "  -e           Enterprise edition"
    echo "  -h           Show help"
}

# ── 配置参数 ─────────────────────────────────────────────────────────────────

# 冷升级需要测试的基准版本
COLD_VERSIONS="3.3.6.0 3.3.8.0 3.4.0.0"

# 绿色版本 HTTP 服务器基础地址
GREEN_HTTP_BASE="http://192.168.1.131/data/nas/TDengine/green_versions"

# 绿色版本本地缓存目录（宿主机）
GREEN_LOCAL_DIR="/green_versions"

# 并发下载锁目录
LOCK_DIR="/tmp/green_versions_locks"

# ── 解析参数 ─────────────────────────────────────────────────────────────────

ent=0
WORKDIR=""
LOG_DIR=""

while getopts "w:l:eh" opt; do
    case $opt in
        w) WORKDIR=$OPTARG ;;
        l) LOG_DIR=$OPTARG ;;
        e) ent=1 ;;
        h) usage; exit 0 ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$WORKDIR" ]; then
    echo "ERROR: -w WORKDIR is required"
    usage
    exit 1
fi

if [ ! -d "$WORKDIR" ]; then
    echo "ERROR: WORKDIR does not exist: $WORKDIR"
    exit 1
fi

if [ -z "$LOG_DIR" ]; then
    LOG_DIR="$WORKDIR/upgrade_compat_logs"
fi

mkdir -p "$LOG_DIR"
mkdir -p "$GREEN_LOCAL_DIR"
mkdir -p "$LOCK_DIR"

echo "======================================================"
echo "  Upgrade Compatibility Test"
echo "======================================================"
echo "  WORKDIR         : $WORKDIR"
echo "  LOG_DIR         : $LOG_DIR"
echo "  GREEN_LOCAL_DIR : $GREEN_LOCAL_DIR"
echo "  COLD_VERSIONS   : $COLD_VERSIONS"
echo "  Enterprise      : $ent"
echo "======================================================"

# ── Step 1: 下载绿色版本（带缓存）──────────────────────────────────────────

function download_version() {
    local version="$1"
    local target_dir="$GREEN_LOCAL_DIR/$version"
    local lock_file="$LOCK_DIR/${version}.lock"
    local start_time
    start_time=$(date +%s)

    # 快速检查：缓存已存在直接跳过
    if [ -d "$target_dir" ] && [ "$(ls -A "$target_dir" 2>/dev/null)" ]; then
        echo "[green_versions] $version: already cached at $target_dir, skip download"
        return 0
    fi

    # 使用 flock 防止并发下载同一版本
    (
        flock -x 200

        # flock 内再次检查（防止等锁期间已被其他进程下载完）
        if [ -d "$target_dir" ] && [ "$(ls -A "$target_dir" 2>/dev/null)" ]; then
            echo "[green_versions] $version: already cached (post-lock check), skip download"
            return 0
        fi

        echo "[green_versions] $version: downloading from $GREEN_HTTP_BASE/$version/ ..."
        mkdir -p "$target_dir"

        # 解析 HTTP 目录列表，获取文件名列表
        local file_list
        file_list=$(curl -fsSL "$GREEN_HTTP_BASE/$version/" \
            | sed -n 's/.*href="\([^"]*\)".*/\1/p' \
            | grep -v '/$\|^\.\.$\|^\.$\|^http\|^/' \
            | grep -v '?' \
            | sort -u)

        if [ -z "$file_list" ]; then
            echo "[green_versions] ERROR: $version: failed to list files from $GREEN_HTTP_BASE/$version/"
            rm -rf "$target_dir"
            return 1
        fi

        echo "[green_versions] $version: files to download: $(echo "$file_list" | tr '\n' ' ')"

        local failed=0
        for fname in $file_list; do
            local url="$GREEN_HTTP_BASE/$version/$fname"
            local dest="$target_dir/$fname"
            echo "[green_versions] $version: downloading $fname ..."
            if ! curl -fsSL "$url" -o "$dest"; then
                echo "[green_versions] ERROR: $version: failed to download $fname from $url"
                failed=1
                break
            fi
            # 对二进制文件赋予执行权限
            case "$fname" in
                taosd|taos|taosadapter|taosBenchmark)
                    chmod +x "$dest"
                    ;;
                *.so*)
                    chmod +x "$dest"
                    ;;
            esac
        done

        if [ $failed -ne 0 ]; then
            echo "[green_versions] $version: download failed, removing partial directory"
            rm -rf "$target_dir"
            return 1
        fi

        local end_time
        end_time=$(date +%s)
        echo "[green_versions] $version: download completed in $((end_time - start_time))s → $target_dir"

    ) 200>"$lock_file"

    return $?
}

echo ""
echo "=== Step 1/2: Downloading green versions ==="
download_failed=0
for ver in $COLD_VERSIONS; do
    download_version "$ver" || {
        echo "ERROR: Failed to download green version $ver"
        download_failed=1
    }
done

if [ $download_failed -ne 0 ]; then
    echo "ERROR: One or more green version downloads failed, aborting."
    exit 1
fi
echo "=== Step 1/2: All green versions ready ==="

# ── Step 2: 确定路径（企业版 vs OSS）────────────────────────────────────────

if [ $ent -ne 0 ]; then
    # 企业版：TDinternal 整体挂载
    INTERNAL_REPDIR="$WORKDIR/TDinternal"
    DEBUGPATH_DIR="$WORKDIR/debugNoSan"
    # 容器内路径（与 run_container.sh -e 一致）
    CONTAINER_REP_MOUNT="$INTERNAL_REPDIR:/home/TDinternal"
    CONTAINER_DEBUG_MOUNT="$DEBUGPATH_DIR:/home/TDinternal/debug"
    CONTAINER_SCRIPT="/home/TDinternal/community/test/ci/run_upgrade_compat_container.sh -e"
else
    # OSS 版：TDengine 挂载
    INTERNAL_REPDIR="$WORKDIR/TDengine"
    DEBUGPATH_DIR="$WORKDIR/debugNoSan"
    CONTAINER_REP_MOUNT="$INTERNAL_REPDIR:/home/TDengine"
    CONTAINER_DEBUG_MOUNT="$DEBUGPATH_DIR:/home/TDengine/debug"
    CONTAINER_SCRIPT="/home/TDengine/test/ci/run_upgrade_compat_container.sh"
fi

if [ ! -d "$INTERNAL_REPDIR" ]; then
    echo "ERROR: Repo directory not found: $INTERNAL_REPDIR"
    exit 1
fi

if [ ! -d "$DEBUGPATH_DIR" ]; then
    echo "ERROR: Debug build directory not found: $DEBUGPATH_DIR"
    exit 1
fi

# ── Step 3: 在独立 Docker 容器中运行升级兼容性测试 ─────────────────────────

echo ""
echo "=== Step 2/2: Running upgrade compatibility tests in Docker ==="
echo ""

docker_cmd="docker run --rm
    -v ${CONTAINER_REP_MOUNT}
    -v ${CONTAINER_DEBUG_MOUNT}
    -v ${GREEN_LOCAL_DIR}:/green_versions:ro
    -v ${LOG_DIR}:/upgrade_logs
    --ulimit core=-1
    tdengine-ci:0.1
    $CONTAINER_SCRIPT"

echo "Docker command:"
echo "$docker_cmd"
echo ""

eval "$docker_cmd"
ret=$?

echo ""
echo "======================================================"
if [ $ret -eq 0 ]; then
    echo "  Upgrade Compatibility Test: PASS"
else
    echo "  Upgrade Compatibility Test: FAIL (exit code: $ret)"
fi
echo "  Log dir: $LOG_DIR"
echo "======================================================"

exit $ret
