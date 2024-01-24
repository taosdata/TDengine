#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -t thread number"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -s build with sanitizer"
    echo -e "\t -h help"
}

ent=0
while getopts "w:d:c:t:o:s:eh" opt; do
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
        s)
            buildSan=$OPTARG
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

#select whether the compilation environment  includes sanitizer
if [ "${buildSan}" == "y" ]; then
    DEBUGPATH="debugSan"
elif [[ "${buildSan}" == "n" ]] || [[ "${case_build_san}" == "" ]]; then
    DEBUGPATH="debugNoSan"
else
    usage
    exit 1
fi

if [ $ent -ne 0 ]; then
    # enterprise edition
    extra_param="$extra_param -e"
    INTERNAL_REPDIR=$WORKDIR/TDinternal
    REPDIR=$INTERNAL_REPDIR/community
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/home/TDinternal/community
    SIM_DIR=/home/TDinternal/sim
    REP_MOUNT_PARAM="$INTERNAL_REPDIR:/home/TDinternal"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDinternal/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
else
    # community edition
    REPDIR=$WORKDIR/TDengine
    REPDIR_DEBUG=$WORKDIR/$DEBUGPATH/
    CONTAINER_TESTDIR=/home/TDengine
    SIM_DIR=/home/TDengine/sim
    REP_MOUNT_PARAM="$REPDIR:/home/TDengine"
    REP_MOUNT_DEBUG="${REPDIR_DEBUG}:/home/TDengine/debug/"
    REP_MOUNT_LIB="${REPDIR_DEBUG}/build/lib:/home/TDinternal/debug/build/lib:ro"
fi

ulimit -c unlimited

TMP_DIR=$WORKDIR/tmp
SOURCEDIR=$WORKDIR/src
MOUNT_DIR=""
# packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
rm -rf ${TMP_DIR}/thread_volume/$thread_no/sim
mkdir -p $SOURCEDIR
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/$exec_dir" ]; then
    subdir=`echo "$exec_dir"|cut -d/ -f1`
    echo "cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
    cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/
fi

# if [ ! -f "${SOURCEDIR}/${packageName}" ]; then
#      wget -P  ${SOURCEDIR} https://taosdata.com/assets-download/3.0/${packageName}
# fi

MOUNT_DIR="$TMP_DIR/thread_volume/$thread_no/$exec_dir:$CONTAINER_TESTDIR/tests/$exec_dir"
echo "$thread_no -> ${exec_dir}:$cmd"
coredump_dir=`cat /proc/sys/kernel/core_pattern | xargs dirname`

docker run \
    -v $REP_MOUNT_PARAM \
    -v $REP_MOUNT_DEBUG \
    -v $REP_MOUNT_LIB \
    -v $MOUNT_DIR \
    -v ${SOURCEDIR}:/usr/local/src/ \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${SIM_DIR}" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:$coredump_dir \
    --rm --ulimit core=-1 taos_test:v1.0 $CONTAINER_TESTDIR/tests/parallel_test/run_case.sh -d "$exec_dir" -c "$cmd" $extra_param
ret=$?
exit $ret

