#!/bin/bash

CONTAINER_TESTDIR=/home/community
# CONTAINER_TESTDIR=/root/tang/repository/TDengine

# export PATH=$PATH:$CONTAINER_TESTDIR/debug/build/bin

function usage() {
    echo "$0"
    echo -e "\t -d execution dir"
    echo -e "\t -c command"
    echo -e "\t -h help"
}

while getopts "d:c:h" opt; do
    case $opt in
        d)
            exec_dir=$OPTARG
            ;;
        c)
            cmd=$OPTARG
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

go env -w GOPROXY=https://goproxy.cn
echo "StrictHostKeyChecking no" >>/etc/ssh/ssh_config
ln -s  /home/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
npm config -g set unsafe-perm
npm config -g set registry https://registry.npm.taobao.org
mkdir -p /home/sim/tsim
mkdir -p /var/lib/taos/subscribe
rm -rf ${CONTAINER_TESTDIR}/src/connector/nodejs/node_modules
rm -rf ${CONTAINER_TESTDIR}/tests/examples/nodejs/node_modules
rm -rf ${CONTAINER_TESTDIR}/tests/connectorTest/nodejsTest/nanosupport/node_modules
# ln -s /home/node_modules ${CONTAINER_TESTDIR}/src/connector/nodejs/
# ln -s /home/node_modules ${CONTAINER_TESTDIR}/tests/examples/nodejs/
# ln -s /home/node_modules ${CONTAINER_TESTDIR}/tests/connectorTest/nodejsTest/nanosupport/
# echo "$cmd"|grep -q "nodejs"
# if [ $? -eq 0 ]; then
#     cd $CONTAINER_TESTDIR/src/connector/nodejs
#     npm install node-gyp-build@4.3.0 --ignore-scripts
# fi

cd $CONTAINER_TESTDIR/tests/$exec_dir
ulimit -c unlimited

$cmd
RET=$?

if [ $RET -ne 0 ]; then
    pwd
fi

exit $RET

