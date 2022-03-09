#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -t thread number"
    echo -e "\t -h help"
}

while getopts "w:d:c:t:h" opt; do
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

ulimit -c unlimited

INTERNAL_REPDIR=$WORKDIR/TDinternal
REPDIR=$INTERNAL_REPDIR/community
CONTAINER_TESTDIR=/home/community
TMP_DIR=$WORKDIR/tmp

MOUNT_DIR=""
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/sim/tsim
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/node_modules
mkdir -p ${TMP_DIR}/thread_volume/$thread_no/coredump
rm -rf ${TMP_DIR}/thread_volume/$thread_no/coredump/*
if [ ! -d "${TMP_DIR}/thread_volume/$thread_no/$exec_dir" ]; then
    subdir=`echo "$exec_dir"|cut -d/ -f1`
    echo "cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/"
    cp -rf ${REPDIR}/tests/$subdir ${TMP_DIR}/thread_volume/$thread_no/
fi
MOUNT_DIR="$TMP_DIR/thread_volume/$thread_no/$exec_dir:$CONTAINER_TESTDIR/tests/$exec_dir"
echo "$thread_no -> ${exec_dir}:$cmd"
echo "$cmd"|grep -q "nodejs"
if [ $? -eq 0 ]; then
    MOUNT_NODE_MOD="-v $TMP_DIR/thread_volume/$thread_no/node_modules:${CONTAINER_TESTDIR}/src/connector/nodejs/node_modules \
-v $TMP_DIR/thread_volume/$thread_no/node_modules:${CONTAINER_TESTDIR}/tests/examples/nodejs/node_modules \
-v $TMP_DIR/thread_volume/$thread_no/node_modules:${CONTAINER_TESTDIR}/tests/connectorTest/nodejsTest/nanosupport/node_modules"
fi
if [ -f "$REPDIR/src/plugins/taosadapter/example/config/taosadapter.toml" ]; then
    TAOSADAPTER_TOML="-v $REPDIR/src/plugins/taosadapter/example/config/taosadapter.toml:/etc/taos/taosadapter.toml:ro"
fi

docker run \
    -v $REPDIR/tests:$CONTAINER_TESTDIR/tests \
    -v $MOUNT_DIR \
    -v "$TMP_DIR/thread_volume/$thread_no/sim:${CONTAINER_TESTDIR}/sim" \
    -v ${TMP_DIR}/thread_volume/$thread_no/coredump:/home/coredump \
    -v $INTERNAL_REPDIR/debug:/home/debug:ro \
    -v $REPDIR/deps:$CONTAINER_TESTDIR/deps:ro \
    -v $REPDIR/src:$CONTAINER_TESTDIR/src \
    -v $REPDIR/src/inc/taos.h:/usr/include/taos.h:ro \
    $TAOSADAPTER_TOML \
    -v $REPDIR/examples:$CONTAINER_TESTDIR/tests/examples \
    -v $REPDIR/snap:$CONTAINER_TESTDIR/snap:ro \
    -v $REPDIR/alert:$CONTAINER_TESTDIR/alert:ro \
    -v $REPDIR/packaging/cfg/taos.cfg:/etc/taos/taos.cfg:ro \
    -v $REPDIR/packaging:$CONTAINER_TESTDIR/packaging:ro \
    -v $REPDIR/README.md:$CONTAINER_TESTDIR/README.md:ro \
    -v $REPDIR/src/connector/python/taos:/usr/local/lib/python3.8/site-packages/taos:ro \
    -e LD_LIBRARY_PATH=/home/debug/build/lib:/home/debug/build/lib64 \
    -e PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/debug/build/bin:/usr/local/go/bin:/usr/local/node-v12.20.0-linux-x64/bin:/usr/local/apache-maven-3.8.4/bin:/usr/local/jdk1.8.0_144/bin \
    -e JAVA_HOME=/usr/local/jdk1.8.0_144 \
    --rm --ulimit core=-1 taos_test:v1.0 $CONTAINER_TESTDIR/tests/parallel_test/run_case.sh -d "$exec_dir" -c "$cmd"
ret=$?
exit $ret

