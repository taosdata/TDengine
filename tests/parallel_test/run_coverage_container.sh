#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir"
    echo -e "\t -b branch id"
    echo -e "\t -t test type (optional, use 'taostools' for taos-tools coverage)"
    echo -e "\t -h help"
}

while getopts "d:b:t:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
            ;;
        b)
            branch_name_id=$OPTARG
            ;;
        t)
            test_type=$OPTARG
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
    usage
    exit 1
fi

if [ -z "$WORKDIR" ]; then
    usage
    exit 1
fi

 # enterprise edition
INTERNAL_REPDIR=$WORKDIR/TDinternal
REPDIR_DEBUG=$WORKDIR/debugNoSan/

REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
REP_MOUNT_PARAM="${INTERNAL_REPDIR}:/home/TDinternal"

CONTAINER_TESTDIR=/home/TDinternal/community


ulimit -c unlimited

# 根据 test_type 选择不同的测试脚本
if [ "$test_type" = "taostools" ]; then
    coverage_script="run_workflow_coverage_taostools.sh"
else
    coverage_script="run_workflow_coverage_tdengine.sh"
fi

docker run --privileged=true \
    --name taos_coverage_tdengine \
    -v /var/lib/jenkins/workspace/TDinternal/:/home/TDinternal/ \
    -v /var/lib/jenkins/workspace/debugNoSan/:/home/TDinternal/debug \
    --rm --ulimit core=-1 tdengine-ci:0.1 sh -c "bash ${CONTAINER_TESTDIR}/tests/parallel_test/${coverage_script} -b ${branch_name_id} " 


ret=$?
exit $ret

