#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir"
    echo -e "\t -h help"
}

while getopts "d:h" opt; do
    case $opt in
        d)
            WORKDIR=$OPTARG
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


 # enterprise edition
INTERNAL_REPDIR=$WORKDIR/TDinternal

REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"

CONTAINER_TESTDIR=/home/TDinternal/community

check_assert_scripts="$CONTAINER_TESTDIR/tests/ci/count_assert.py" 

ulimit -c unlimited
cat << EOF
docker run \
    -v $REP_MOUNT_PARAM \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $check_assert_scripts
EOF
docker run \
    -v "$REP_MOUNT_PARAM" \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $check_assert_scripts

ret=$?
exit $ret

