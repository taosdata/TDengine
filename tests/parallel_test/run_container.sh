#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -t thread number"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -h help"
}

ent=0
while getopts "w:d:c:t:o:eh" opt; do
    case $opt in
        w)
            WORKDIR=$OPTARG
            ;;
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
            ;;
        t)
            thread_no=$OPTARG
            ;;
        e)
            ent=1
            ;;
        o)
            extra_param="-o $OPTARG"
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
if [ -z "$exec_dir" ]; then
    usage
    exit 1
fi
if [ -z "$cmd" ]; then
    usage
    exit 1
fi
if [ -z "$thread_no" ]; then
    usage
    exit 1
fi
if [ $ent -ne 0 ]; then
    # enterprise edition
    extra_param="$extra_param -e"
    INTERNAL_REPDIR=$WORKDIR/TDinternal
    REPDIR=$INTERNAL_REPDIR/community
    CONTAINER_TESTDIR=/home/TDinternal/community
    SIM_DIR=/home/TDinternal/sim
    REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"
else
    # community edition
    REPDIR=$WORKDIR/TDengine
    CONTAINER_TESTDIR=/home/TDengine
    SIM_DIR=/home/TDengine/sim
    REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
fi

ulimit -c unlimited

TMP_DIR=$WORKDIR/tmp

MOUNT_DIR=""
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/$exec_dir" ]; then
    subdir=`echo "$exec_dir"|cut -d/ -f1`
    echo "cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
    cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/
fi
MOUNT_DIR="$TMP_DIR/thread_volume/$thread_no/$exec_dir:$CONTAINER_TESTDIR/tests/$exec_dir"
echo "$thread_no -> ${exec_dir}:$cmd"
coredump_dir=`cat /proc/sys/kernel/core_pattern | xargs dirname`

docker run \
    -v $REP_MOUNT_PARAM \
    -v $MOUNT_DIR \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    -v $WORKDIR/taos-connector-python/taos:/usr/local/lib/python3.8/site-packages/taos:ro \
    -v $WORKDIR/taos-connector-python/taosrest:/usr/local/lib/python3.8/site-packages/taosrest:ro \
    --rm --ulimit core=-1 taos_test:v1.0 $CONTAINER_TESTDIR/tests/parallel_test/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
exit $ret

