#!/bin/bash
# 容器内 PID 1 的命令行包含 taos-community，其中 "taos" 是独立单词（前后是非单词字符 / -）。
# 测试中的 closeBin("taos") 用 ps -ef|grep -w taos 会匹配 PID 1，kill -9 1 内核拒绝，导致死循环。
# Fix: 首次运行时创建中性路径的软链并重新 exec 自身，这样 PID 1 的命令行就不包含 taos 了。
if [[ "$0" != "/tmp/run_ci.sh" ]]; then
    ln -sfn "$(realpath "$0")" /tmp/run_ci.sh
    exec /tmp/run_ci.sh "$@"
fi

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

# ── 自动检测容器内路径布局 ─────────────────────────────────────────────────
# tsdb 仓库 CI：/mnt/tsdb/debug/build/bin, /mnt/tsdb/source/taos-community
# TDinternal CI（兼容旧镜像）：/home/TDinternal/debug/build/bin
if [ -d "/mnt/tsdb/debug/build/bin" ]; then
    _DEBUG_BASE="/mnt/tsdb/debug"
    _SOURCE_BASE="/mnt/tsdb/source/taos-community"
    _SIM_BASE="/mnt/tsdb/sim"
else
    _DEBUG_BASE="/home/TDinternal/debug"
    _SOURCE_BASE="/home/TDinternal/community"
    _SIM_BASE="/home/TDinternal/sim"
fi

export PATH=$PATH:${_DEBUG_BASE}/build/bin
export LD_LIBRARY_PATH=${_DEBUG_BASE}/build/lib
ln -s ${_DEBUG_BASE}/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosws.so /usr/lib/libtaosws.so 2>/dev/null
ln -s ${_DEBUG_BASE}/build/lib/libtaosws.so /usr/lib/libtaosws.so.1 2>/dev/null
ln -s ${_SOURCE_BASE}/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s ${_SOURCE_BASE}/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null
# 将 build/lib 写入 ld.so.conf.d 并刷新缓存，确保 LD_LIBRARY_PATH 未被继承的
# 子进程（sh -c / Python subprocess / docker exec bash 等）也能找到正确的库。
echo "${_DEBUG_BASE}/build/lib" > /etc/ld.so.conf.d/tdengine.conf
ldconfig 2>/dev/null || true
CONTAINER_TESTDIR=${_SOURCE_BASE}
mkdir -p /var/lib/taos/subscribe
mkdir -p /var/log/taos
mkdir -p /var/lib/taos
mkdir -p /etc/taos
# 兼容性测试（compatibility_rolling_upgrade 等）依赖 /etc/taos/taosadapter.toml：
# 旧版 install.sh 会尝试备份该文件，再创建新版；若文件不存在则备份失败，
# 旧版 install.sh 在容器内安装失败时也不会创建它，导致后续 cp 静默失败
# → alter_string_in_file 抛 FileNotFoundError。
# 此处预先写入最小默认模板（含测试代码 alter_string_in_file 所需的两个关键字段），
# 若旧版 install.sh 成功，会覆盖为真实配置；若失败，至少保留此占位文件供 cp 使用。
if [ ! -f /etc/taos/taosadapter.toml ]; then
    cat > /etc/taos/taosadapter.toml << 'ADAPTER_EOF'
# taosadapter default configuration (CI container placeholder)
taosConfigDir = ""

[log]
#path = "/var/log/taos"
level = "info"
ADAPTER_EOF
fi
# 同理：提供 /usr/bin/taosd 软链接，供旧版 install.sh 服务探测及兼容性测试直接调用；
# 旧版 install.sh 安装成功后会覆盖此链接为旧版二进制。
ln -sf "${_DEBUG_BASE}/build/bin/taosd" /usr/bin/taosd 2>/dev/null || true
ln -sf "${_DEBUG_BASE}/build/bin/taosadapter" /usr/bin/taosadapter 2>/dev/null || true
ln -sf "${_DEBUG_BASE}/build/bin/taosBenchmark" /usr/bin/taosBenchmark 2>/dev/null || true
# 配置 npm registry 走 Nexus 代理（避免直连外网 ECONNRESET）
npm config set registry https://nexus.tdengine.net/repository/npm/ 2>/dev/null || true

# Fix Ubuntu 24.04 / kernel 6.8: apport pipe in core_pattern causes exit=123
# --privileged containers can write /proc/sys even if host runner is non-root
# Core files are written to /corefile (volume-mounted to host)
_COREDIR="/corefile"
mkdir -p "${_COREDIR}"
_CORE_PAT=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
if echo "${_CORE_PAT}" | grep -q '^|'; then
    echo "[run_case] core_pattern has pipe (apport), overriding to ${_COREDIR}/core.%e.%p"
    echo "${_COREDIR}/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null \
        || echo "[run_case] WARNING: cannot write core_pattern"
fi

cd "$CONTAINER_TESTDIR/tests/${exec_dir}"|| { echo "Can't enter the target dirctory: ${CONTAINER_TESTDIR}/tests/${exec_dir}"; exit 1; }
ulimit -c unlimited

_SIM_DIR="${_SIM_BASE}"
_LIB_DIR="${_DEBUG_BASE}/build/lib"

# 导出标准路径环境变量，供被调用的 Python 测试脚本（army/system-test 等）使用。
# 当测试命令不经过 pytest.sh 包装时（如直接 python3 ./test.py），
# 框架内的 getPath()/getBuildPath()/binPath()/init() 需要这些变量来定位 taosd。
export BUILD_DIR="${_DEBUG_BASE}"
export SIM_DIR="${_SIM_BASE}"
export CODE_DIR="${CONTAINER_TESTDIR}/tests/${exec_dir}"

md5sum /usr/lib/libtaos.so.1        2>/dev/null || echo "libtaos.so.1 not found in /usr/lib"
md5sum "${_LIB_DIR}/libtaos.so"     2>/dev/null || echo "libtaos.so not found in ${_LIB_DIR}"
md5sum /usr/lib/libtaosnative.so.1  2>/dev/null || echo "libtaosnative.so.1 not found in /usr/lib"
md5sum "${_LIB_DIR}/libtaosnative.so" 2>/dev/null || echo "libtaosnative.so not found in ${_LIB_DIR}"

#get python connector and update: taospy 2.8.9 taos-ws-py 0.6.9
pip3 install taospy==2.8.9
pip3 install taos-ws-py==0.6.9
pip3 install pyotp

# ── ASAN 运行时自动检测 ───────────────────────────────────────────────────────
# 若 libtaos.so 是 ASAN 编译产物，python3 等非 ASAN 二进制在 dlopen 它之前需先
# preload libasan；否则 ASan 会报 "runtime does not come first" 并以 exit=1 退出。
# ldd 解析 DT_NEEDED，awk 提取 libasan 的绝对路径（格式: libasan.so.N => /path (addr)）。
if [ -z "${LD_PRELOAD:-}" ]; then
    _asan_lib=$(ldd "${_LIB_DIR}/libtaos.so" 2>/dev/null \
                    | awk '/libasan/{print $3; exit}')
    if [ -n "${_asan_lib}" ] && [ -f "${_asan_lib}" ]; then
        export LD_PRELOAD="${_asan_lib}"
        echo "[run_case] ASAN build detected, LD_PRELOAD=${LD_PRELOAD}"
    fi
fi

$TIMEOUT_CMD $cmd
RET=$?
echo "cmd exit code: $RET"
md5sum /usr/lib/libtaos.so.1        2>/dev/null || true
md5sum "${_LIB_DIR}/libtaos.so"     2>/dev/null || true
md5sum /usr/lib/libtaosnative.so.1  2>/dev/null || true
md5sum "${_LIB_DIR}/libtaosnative.so" 2>/dev/null || true

mkdir -p ${_SIM_DIR}/var_taoslog
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

