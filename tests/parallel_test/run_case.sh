#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -e enterprise edition"
    echo -e "\t -o default timeout value"
    echo -e "\t -h help"
}

ent=0
while getopts "d:c:o:eh" opt; do
    case $opt in
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
            ;;
        o)
            TIMEOUT_CMD="timeout $OPTARG"
            ;;
        e)
            ent=1
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

if [ -z "$exec_dir" ]; then
    usage
    exit 0
fi
if [ -z "$cmd" ]; then
    usage
    exit 0
fi

if [ $ent -eq 0 ]; then
    export PATH=$PATH:/home/TDengine/debug/build/bin
    export LD_LIBRARY_PATH=/home/TDengine/debug/build/lib
    ln -s /home/TDengine/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
    ln -s /home/TDengine/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
    ln -s /home/TDengine/include/client/taos.h /usr/include/taos.h 2>/dev/null
    CONTAINER_TESTDIR=/home/TDengine
else
    export PATH=$PATH:/home/TDinternal/debug/build/bin
    export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib
    ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
    ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
    ln -s /home/TDinternal/community/include/client/taos.h /usr/include/taos.h 2>/dev/null
    CONTAINER_TESTDIR=/home/TDinternal/community
fi
mkdir -p /var/lib/taos/subscribe
mkdir -p /var/log/taos
mkdir -p /var/lib/taos

cd $CONTAINER_TESTDIR/tests/$exec_dir
ulimit -c unlimited

md5sum /usr/lib/libtaos.so.1
md5sum /home/TDinternal/debug/build/lib/libtaos.so
$TIMEOUT_CMD $cmd
RET=$?
echo "cmd exit code: $RET"
md5sum /usr/lib/libtaos.so.1
md5sum /home/TDinternal/debug/build/lib/libtaos.so

if [ $RET -ne 0 ]; then
    pwd
fi

exit $RET

