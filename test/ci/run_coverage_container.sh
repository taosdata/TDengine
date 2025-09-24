#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir (default: /var/lib/jenkins/workspace)"
    echo -e "\t -b branch id for coverage (required)"
    echo -e "\t -c container name (default: taos_coverage_tdengine)"
    echo -e "\t -i docker image (default: tdengine-ci:0.1)"
    echo -e "\t -h help"
}

WORKDIR="/var/lib/jenkins/workspace"
CONTAINER_NAME="taos_coverage_tdengine"
DOCKER_IMAGE="tdengine-ci:0.1"
branch_name_id=""

while getopts "d:b:t:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
            ;;
        b)
            branch_name_id=$OPTARG
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
DEBUG_DIR=$WORKDIR/debugNoSan/

CONTAINER_TDINTERNAL_DIR="/home/TDinternal"
CONTAINER_DEBUG_DIR="$CONTAINER_TDINTERNAL_DIR/debug"
CONTAINER_TESTDIR="$CONTAINER_INTERNAL_DIR/community"

ulimit -c unlimited

echo "WORKDIR = $WORKDIR"
echo "TDINTERNAL_DIR = $TDINTERNAL_DIR"
echo "DEBUG_DIR = $DEBUG_DIR"
echo "branch_name_id = $branch_name_id"

docker run \
    --privileged=true \
    --name "$CONTAINER_NAME" \
    -v "$INTERNAL_REPDIR:$CONTAINER_INTERNAL_DIR" \
    -v "$DEBUG_DIR:$CONTAINER_DEBUG_DIR" \
    --rm \
    --ulimit core=-1 \
    "$DOCKER_IMAGE" \
    sh -c "bash $CONTAINER_TESTDIR/test/ci/run_coverage_diff.sh -b $branch_name_id"

ret=$?

if [ $ret -eq 0 ]; then
    echo "Coverage test completed successfully."
else
    echo "Coverage test failed. $ret"
fi

exit $ret