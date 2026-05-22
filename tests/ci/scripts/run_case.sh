#!/bin/bash
# =============================================================================
# run_case.sh — 在宿主机（非容器）直接运行 tsdb 仓库的 pytest 用例
#
# 适用场景:
#   开发者在本机编译完成后，快速运行单条或多条 pytest 用例进行验证，
#   无需启动 Docker 容器。脚本会自动设置 PATH、LD_LIBRARY_PATH、WORK_DIR
#   等环境变量，使测试框架能正确找到 taosd 二进制和 sim 工作目录。
#
# 前置条件:
#   1. 已编译 tsdb（debug/build/bin/taosd 存在）
#   2. Python 3.8+ 可用（支持 pyenv 自动检测）
#   3. 当前用户对 sim/ 目录有读写权限
#
# 用法:
#   ./tests/ci/scripts/run_case.sh [OPTIONS] <test_path> [pytest_args...]
#
# 示例:
#   # 运行单个用例（最快，不带 ASAN）
#   ./tests/ci/scripts/run_case.sh cases/01-DataTypes/test_datatype_bigint.py
#
#   # 运行前清理 sim/ 残留数据
#   ./tests/ci/scripts/run_case.sh --clean cases/09-DataQuerying/test_query_basic.py
#
#   # 带 ASAN 检测内存问题（通过 pytest.sh 调用）
#   ./tests/ci/scripts/run_case.sh --asan cases/01-DataTypes/test_datatype_bigint.py
#
#   # 传递额外 pytest 参数（如 -N 设置 dnode 数量、-s 显示 stdout）
#   ./tests/ci/scripts/run_case.sh cases/01-DataTypes/test_datatype_bigint.py -N 3 -s
#
#   # 使用自定义编译目录
#   TAOS_BIN_PATH=/data/tsdb/debug-release/build/bin \
#     ./tests/ci/scripts/run_case.sh cases/01-DataTypes/test_datatype_bigint.py
#
# 选项:
#   --clean          运行前删除并重建 sim/ 目录（清除上次测试残留）
#   --no-asan        （默认）直接调用 pytest，不注入 ASAN，启动最快
#   --asan           通过 ci/pytest.sh 调用，LD_PRELOAD libasan 检测内存错误
#   --via-pytest-sh  等同于 --asan，兼容旧有 CI 写法
#   -h, --help       显示此帮助信息
#
# 环境变量（均可选，有合理默认值）:
#   TSDB_DIR      — tsdb 仓库根目录（默认: 从脚本位置向上三级推导）
#   TAOS_BIN_PATH — taosd/taosc 所在 bin 目录（默认: $TSDB_DIR/debug/build/bin）
#   WORK_DIR      — 测试 sim 工作目录，存放 dnode 数据/日志（默认: $TSDB_DIR/sim）
#
# 目录布局:
#   $TSDB_DIR/
#   ├── debug/build/bin/taosd      ← TAOS_BIN_PATH
#   ├── debug/build/lib/libtaos.so ← LD_LIBRARY_PATH
#   ├── sim/                       ← WORK_DIR (dnode1/cfg, dnode1/log, ...)
#   ├── source/taos-community/test/ ← TEST_DIR (pytest 工作目录)
#   └── tests/ci/scripts/run_case.sh ← 本脚本
# =============================================================================

set -e

# ── 确保 pyenv 可用（非交互 shell 可能未初始化） ─────────────────────────────
if [[ -d "${HOME}/.pyenv" ]]; then
    export PYENV_ROOT="${HOME}/.pyenv"
    export PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"
    # 使用 pyenv global 版本，忽略目录级 .python-version（可能指定未安装的版本）
    export PYENV_VERSION="$(pyenv global)"
fi

# ── 自动推导 TSDB_DIR ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -z "${TSDB_DIR}" ]]; then
    # tests/ci/scripts/run_case.sh → tsdb root is three levels up
    TSDB_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
fi

# ── 参数解析 ─────────────────────────────────────────────────────────────────
usage() {
    cat <<'EOF'
用法: run_case.sh [OPTIONS] <test_path> [pytest_args...]

在宿主机（非容器）直接运行 tsdb 仓库的 pytest 用例。

位置参数:
  <test_path>        测试文件相对于 test/ 目录的路径
                     例: cases/01-DataTypes/test_datatype_bigint.py
  [pytest_args...]   额外传递给 pytest 的参数（如 -s, -v, -N 3, -k "xxx"）

选项:
  --clean            运行前删除并重建 sim/ 目录，清除上次测试残留数据
  --no-asan          （默认模式）直接调用 pytest，不注入 ASAN，启动最快
  --asan             通过 ci/pytest.sh 调用，LD_PRELOAD libasan 检测内存错误
  --via-pytest-sh    等同于 --asan，兼容旧有 CI 写法
  -h, --help         显示此帮助信息并退出

环境变量（均可选）:
  TSDB_DIR           tsdb 仓库根目录（默认: 从脚本位置向上三级推导）
  TAOS_BIN_PATH      taosd 所在 bin 目录（默认: $TSDB_DIR/debug/build/bin）
  WORK_DIR           测试 sim 工作目录（默认: $TSDB_DIR/sim）

示例:
  # 快速运行单个用例
  ./tests/ci/scripts/run_case.sh cases/01-DataTypes/test_datatype_bigint.py

  # 清理后运行，显示详细输出
  ./tests/ci/scripts/run_case.sh --clean cases/09-DataQuerying/test_query_basic.py -v -s

  # 带 ASAN 运行
  ./tests/ci/scripts/run_case.sh --asan cases/12-UDFs/test_udf_restart_taosd.py

  # 指定自定义编译路径
  TAOS_BIN_PATH=/data/tsdb/debug-san/build/bin \
    ./tests/ci/scripts/run_case.sh --asan cases/01-DataTypes/test_datatype_bigint.py
EOF
    exit 0
}

USE_ASAN=false
DO_CLEAN=false
VIA_PYTEST_SH=false
declare -a PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        --asan|--via-pytest-sh)
            USE_ASAN=true
            VIA_PYTEST_SH=true
            shift
            ;;
        --no-asan)
            USE_ASAN=false
            shift
            ;;
        --clean)
            DO_CLEAN=true
            shift
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#PYTEST_ARGS[@]} -eq 0 ]]; then
    echo "错误: 未指定测试文件路径"
    echo ""
    echo "用法: $0 [--asan|--no-asan|--clean] cases/path/test_xxx.py [pytest opts...]"
    echo "运行 '$0 --help' 查看完整帮助"
    exit 1
fi

# ── 路径设置 ──────────────────────────────────────────────────────────────────
TEST_DIR="${TSDB_DIR}/source/taos-community/test"
export TAOS_BIN_PATH="${TAOS_BIN_PATH:-${TSDB_DIR}/debug/build/bin}"
export WORK_DIR="${WORK_DIR:-${TSDB_DIR}/sim}"

# 验证 taosd 存在
if [[ ! -f "${TAOS_BIN_PATH}/taosd" ]]; then
    echo "ERROR: taosd not found at ${TAOS_BIN_PATH}/taosd"
    echo "Set TAOS_BIN_PATH or build first: tools/tsdb-builder/build.sh --image others --src ${TSDB_DIR}"
    exit 1
fi

# ── LD_LIBRARY_PATH ──────────────────────────────────────────────────────────
LIB_PATH="$(dirname "${TAOS_BIN_PATH}")/lib"
export LD_LIBRARY_PATH="${LIB_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export PATH="${TAOS_BIN_PATH}:${PATH}"

# ── 清理 sim 目录 ────────────────────────────────────────────────────────────
if [[ "${DO_CLEAN}" == "true" ]]; then
    echo "[run_case] Cleaning ${WORK_DIR}..."
    rm -rf "${WORK_DIR}"
fi
mkdir -p "${WORK_DIR}"

# ── 安装依赖 ─────────────────────────────────────────────────────────────────
# 使用 marker 文件 + requirements.txt 的 mtime 判断是否需要重新安装
_DEPS_MARKER="${TEST_DIR}/.deps_installed"
if [[ ! -f "${_DEPS_MARKER}" ]] || \
   [[  "${TEST_DIR}/requirements.txt" -nt "${_DEPS_MARKER}" ]]; then
    echo "[run_case] Installing test dependencies..."
    pip3 install -r "${TEST_DIR}/requirements.txt" -q \
        -i https://pypi.tuna.tsinghua.edu.cn/simple \
        --trusted-host pypi.tuna.tsinghua.edu.cn \
        2>&1 | tail -5
    touch "${_DEPS_MARKER}"
fi

# ── 执行测试 ─────────────────────────────────────────────────────────────────
echo "[run_case] TSDB_DIR      : ${TSDB_DIR}"
echo "[run_case] TAOS_BIN_PATH : ${TAOS_BIN_PATH}"
echo "[run_case] WORK_DIR      : ${WORK_DIR}"
echo "[run_case] LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
echo "[run_case] Mode          : $(${USE_ASAN} && echo 'ASAN (via pytest.sh)' || echo 'direct pytest (no ASAN)')"
echo "[run_case] Command       : ${PYTEST_ARGS[*]}"
echo "------------------------------------------------------------------------"

cd "${TEST_DIR}"

if [[ "${VIA_PYTEST_SH}" == "true" ]]; then
    # 通过 pytest.sh 运行（兼容 CI，带 ASAN checkAsan 检查）
    export CI_NO_ASAN=0
    exec ./ci/pytest.sh pytest --clean "${PYTEST_ARGS[@]}"
else
    # 直接调用 pytest（快速模式，不走 ASAN）
    exec python3 -m pytest --clean "${PYTEST_ARGS[@]}"
fi
