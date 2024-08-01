#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d work dir"
    echo -e "\t -b pr and id"
    echo -e "\t -w web server "
    echo -e "\t -f scan file "
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
        f)
            scan_file_name=$OPTARG
            ;;
        w)
            web_server=$OPTARG
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

if [ -z "$scan_file_name" ]; then
    usage
    exit 1
fi
if [ -z "$WORKDIR" ]; then
    usage
    exit 1
fi
if [ -z "$web_server" ]; then
    usage
    exit 1
fi

 # enterprise edition
INTERNAL_REPDIR=$WORKDIR/TDinternal
REPDIR_DEBUG=$WORKDIR/debugNoSan/

REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"

CONTAINER_TESTDIR=/home/TDinternal/community

#scan file log path
scan_temp="$WORKDIR/log/${branch_name_id}/"
docker_scan_temp="/home/${branch_name_id}/"
mkdir -p $scan_temp
mkdir -p $docker_scan_temp


scan_scripts="$CONTAINER_TESTDIR/tests/ci/scan_file_path.py" 
scan_file_name="$docker_scan_temp/docs_changed.txt"

ulimit -c unlimited
cat << EOF
docker run \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $scan_temp:$docker_scan_temp \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $scan_scripts -b "${branch_name_id}"  -f "${scan_file_name}" -w ${web_server}
EOF
docker run \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $scan_temp:$docker_scan_temp \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $scan_scripts -b "${branch_name_id}"  -f "${scan_file_name}" -w ${web_server}


ret=$?
exit $ret

