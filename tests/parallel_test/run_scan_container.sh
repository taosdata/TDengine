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

#scan change file path
scan_changefile_temp_path="$WORKDIR/tmp/${branch_name_id}/"
docker_can_changefile_temp_path="/home/tmp/${branch_name_id}/"
mkdir -p $scan_changefile_temp_path
scan_file_name="$docker_can_changefile_temp_path/docs_changed.txt"

#scan log file path
scan_log_temp_path="$WORKDIR/log/scan_log/"
docker_scan_log_temp_path="/home/scan_log/"
mkdir -p $scan_log_temp_path


scan_scripts="$CONTAINER_TESTDIR/tests/ci/scan_file_path.py" 

ulimit -c unlimited
cat << EOF
docker run \
    -v /root/.cos-local.1:/root/.cos-local.2 \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $scan_changefile_temp_path:$docker_can_changefile_temp_path \
    -v $scan_log_temp_path:$docker_scan_log_temp_path \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $scan_scripts -b "${branch_name_id}"  -f "${scan_file_name}" -w ${web_server}
EOF
docker run \
    -v /root/.cos-local.1:/root/.cos-local.2 \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $scan_changefile_temp_path:$docker_can_changefile_temp_path \
    -v $scan_log_temp_path:$docker_scan_log_temp_path \
    --rm --ulimit core=-1 taos_test:v1.0 python3  $scan_scripts -b "${branch_name_id}"  -f "${scan_file_name}" -w ${web_server}


ret=$?
exit $ret

