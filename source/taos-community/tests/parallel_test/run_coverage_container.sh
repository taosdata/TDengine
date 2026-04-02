#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir"
    echo -e "\t -b branch id"
    echo -e "\t -h help"
}

while getopts "d:b:w:f:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
            ;;
        b)
            branch_name_id=$OPTARG
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

docker run \
    --name taos_coverage \
    -v /var/lib/jenkins/workspace/TDinternal/:/home/TDinternal/ \
    -v /var/lib/jenkins/workspace/debugNoSan/:/home/TDinternal/community/debug \
    --rm --ulimit core=-1 tdengine-ci:0.1 sh -c "bash ${CONTAINER_TESTDIR}/tests/run_workflow_coverage.sh -b ${branch_name_id} " 


ret=$?
exit $ret

