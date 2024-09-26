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

check_assert_scripts="$CONTAINER_TESTDIR/tests/ci/check_void.sh  " 
func_list_file="$CONTAINER_TESTDIR/tests/ci/func.txt"
source_file_path="$CONTAINER_TESTDIR/source/"

ulimit -c unlimited
cat << EOF
docker run \
    -v $REP_MOUNT_PARAM \
    --rm --ulimit core=-1 taos_test:v1.0  "$check_assert_scripts" -c $func_list_file -f $source_file_path
EOF

docker run \
    -v "$REP_MOUNT_PARAM" \
    --rm --ulimit core=-1 taos_test:v1.0  "$check_assert_scripts" -c $func_list_file -f $source_file_path

ret=$?
exit $ret

