#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir (default: /var/lib/jenkins/workspace)"
    echo -e "\t -b branch id for coverage (required)"
    echo -e "\t -l test log dir (optional, for collecting gcda from test cases)"
    echo -e "\t -c container name (default: taos_coverage_tdengine)"
    echo -e "\t -i docker image (default: tdengine-ci:0.1)"
    echo -e "\t -h help"
}

WORKDIR="/var/lib/jenkins/workspace"
CONTAINER_NAME="taos_coverage_tdengine"
DOCKER_IMAGE="tdengine-ci:0.1"
branch_name_id=""
test_log_dir=""

while getopts "d:b:l:c:i:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
            ;;
        b)
            branch_name_id=$OPTARG
            ;;
        l)
            test_log_dir=$OPTARG
            ;;
        c)
            CONTAINER_NAME=$OPTARG
            ;;
        i)
            DOCKER_IMAGE=$OPTARG
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

if [ -z "$branch_name_id" ]; then
    echo "Error: branch id for coverage is required"
    usage
    exit 1
fi

if [ -z "$WORKDIR" ]; then
    echo "Error: work dir is required"
    usage
    exit 1
fi

if [ ! -d "$WORKDIR" ]; then
    echo "Error: $WORKDIR not exist"
    exit 1
fi

TDINTERNAL_DIR=$WORKDIR/TDinternal

# 验证关键目录是否存在
if [ ! -d "$TDINTERNAL_DIR" ]; then
    echo "Error: TDinternal directory not found: $TDINTERNAL_DIR"
    exit 1
fi

CONTAINER_TDINTERNAL_DIR="/home/TDinternal"
CONTAINER_TESTDIR="$CONTAINER_TDINTERNAL_DIR/community"
CONTAINER_LOG_DIR="/home/test_logs"

ulimit -c unlimited

echo "WORKDIR = $WORKDIR"
echo "TDINTERNAL_DIR = $TDINTERNAL_DIR"
echo "branch_name_id = $branch_name_id"
echo "test_log_dir = $test_log_dir"

# 检查是否已存在同名容器，如果存在则删除
if docker ps -a --format "table {{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing existing container: $CONTAINER_NAME"
    docker rm -f "$CONTAINER_NAME" 2>/dev/null
fi

# 构建 Docker 挂载参数
DOCKER_MOUNTS="-v $TDINTERNAL_DIR:$CONTAINER_TDINTERNAL_DIR"

# 挂载测试日志目录（如果存在）- 这里包含所有case的.info文件
if [ -n "$test_log_dir" ] && [ -d "$test_log_dir" ]; then
    DOCKER_MOUNTS="$DOCKER_MOUNTS -v $test_log_dir:$CONTAINER_LOG_DIR"
    echo "Mounting test logs (with .info files): $test_log_dir -> $CONTAINER_LOG_DIR"
fi

echo "Starting Docker container: $CONTAINER_NAME"
echo "Docker mounts: $DOCKER_MOUNTS"

# 构建覆盖率命令参数
COVERAGE_ARGS="-b $branch_name_id"
if [ -n "$test_log_dir" ] && [ -d "$test_log_dir" ]; then
    COVERAGE_ARGS="$COVERAGE_ARGS -l $CONTAINER_LOG_DIR"
fi

echo "Running coverage analysis..."
docker run \
    --privileged=true \
    --name "$CONTAINER_NAME" \
    $DOCKER_MOUNTS \
    --rm --ulimit core=-1 \
    "$DOCKER_IMAGE" \
    sh -c "bash $CONTAINER_TESTDIR/test/ci/cov/run_coverage_diff.sh $COVERAGE_ARGS"

ret=$?

if [ $ret -eq 0 ]; then
    echo "Coverage test completed successfully."
else
    echo "Coverage test failed with exit code: $ret"
fi

# 显示容器状态信息
echo "Container status:"
docker ps -a --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}"

echo "To debug the container:"
echo "  docker exec -it $CONTAINER_NAME /bin/bash"
echo "To view container logs:"
echo "  docker logs $CONTAINER_NAME"
echo "To remove the container:"
echo "  docker rm -f $CONTAINER_NAME"

exit $ret