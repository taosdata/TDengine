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

export PATH=$PATH:/home/TDinternal/debug/build/bin
export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosws.so /usr/lib/libtaosws.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosws.so /usr/lib/libtaosws.so.1 2>/dev/null
ln -s /home/TDinternal/community/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s /home/TDinternal/community/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s /home/TDinternal/community/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s /home/TDinternal/community/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null
# 将 build/lib 写入 ld.so.conf.d 并刷新缓存，确保 LD_LIBRARY_PATH 未被继承的
# 子进程（sh -c / Python subprocess / docker exec bash 等）也能找到正确的库。
echo "/home/TDinternal/debug/build/lib" > /etc/ld.so.conf.d/tdengine.conf
ldconfig 2>/dev/null || true
CONTAINER_TESTDIR=/home/TDinternal/community
mkdir -p /var/lib/taos/subscribe
mkdir -p /var/log/taos
mkdir -p /var/lib/taos
mkdir -p /etc/taos
# 配置 npm registry 走 Nexus 代理（避免直连外网 ECONNRESET）
npm config set registry https://nexus.tdengine.net/repository/npm/ 2>/dev/null || true

# Fix Ubuntu 24.04 / kernel 6.8: apport pipe in core_pattern causes exit=123
# --privileged containers can write /proc/sys even if host runner is non-root
# Core files are written to /home/coredump (volume-mounted to host) so they
# survive container exit and can be collected by the CI runner.
mkdir -p /home/coredump
_CORE_PAT=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
if echo "${_CORE_PAT}" | grep -q '^|'; then
    echo "[run_case] core_pattern has pipe (apport), overriding to /home/coredump/core.%e.%p"
    echo "/home/coredump/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null \
        || echo "[run_case] WARNING: cannot write core_pattern"
fi

cd "$CONTAINER_TESTDIR/tests/${exec_dir}"|| { echo "Can't enter the target dirctory: ${CONTAINER_TESTDIR}/tests/${exec_dir}"; exit 1; }
ulimit -c unlimited

_SIM_DIR="/home/TDinternal/sim"
_LIB_DIR="/home/TDinternal/debug/build/lib"

md5sum /usr/lib/libtaos.so.1        2>/dev/null || echo "libtaos.so.1 not found in /usr/lib"
md5sum "${_LIB_DIR}/libtaos.so"     2>/dev/null || echo "libtaos.so not found in ${_LIB_DIR}"
md5sum /usr/lib/libtaosnative.so.1  2>/dev/null || echo "libtaosnative.so.1 not found in /usr/lib"
md5sum "${_LIB_DIR}/libtaosnative.so" 2>/dev/null || echo "libtaosnative.so not found in ${_LIB_DIR}"

#get python connector and update: taospy 2.8.9 taos-ws-py 0.6.9
pip3 install taospy==2.8.9
pip3 install taos-ws-py==0.6.9
pip3 install pyotp
$TIMEOUT_CMD $cmd
RET=$?
echo "cmd exit code: $RET"
md5sum /usr/lib/libtaos.so.1        2>/dev/null || true
md5sum "${_LIB_DIR}/libtaos.so"     2>/dev/null || true
md5sum /usr/lib/libtaosnative.so.1  2>/dev/null || true
md5sum "${_LIB_DIR}/libtaosnative.so" 2>/dev/null || true

if [ -d "/var/log/taos" ]; then
    cp /var/log/taos/* "${_SIM_DIR}/var_taoslog/" 2>/dev/null || true
fi

if [ -f "${CONTAINER_TESTDIR}/docs/examples/java/jdbc-out.log" ]; then
    cp "${CONTAINER_TESTDIR}/docs/examples/java/jdbc-out.log" "${_SIM_DIR}/var_taoslog/" 2>/dev/null || true
fi

if [ $RET -ne 0 ]; then
    pwd
fi

exit $RET

