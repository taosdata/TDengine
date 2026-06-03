#!/bin/bash
# ======================================================================
# rerun.sh — 在本机或 runner 上运行 / 复现测试用例
#
# 使用场景：
#
#   1. 本地有编译产物，直接运行指定用例：
#      ./rerun.sh "pytest cases/11-Functions/06-System/test_fun_sys_info.py"
#
#   2. 指定 MR 号，从 Nexus 下载构建产物并运行指定用例：
#      ./rerun.sh --mr 42 "pytest cases/11-Functions/06-System/test_fun_sys_info.py"
#
#   3. 在 runner 机器上复现 CI 失败用例（读 fail-logs）：
#      ./rerun.sh --case n4-11-Functions_06-System_test_fun_sys_info
#
#   4. 用指定 MR 的构建复现 CI 失败用例：
#      ./rerun.sh --mr 42 --case n4-11-Functions_06-System_test_fun_sys_info
#
# 选项：
#   --mr  <N>       MR 号，从 Nexus 下载该 MR 的构建产物
#   --case <name>   从 fail-logs 读取 CI 失败用例信息（runner 机器）
#   -d <DIR>        手动指定 debug/debugNoSan 目录（覆盖自动检测）
#   -r <DIR>        手动指定 taos-community 目录（覆盖自动检测）
#   -s              使用 sanitizer 构建（默认 non-sanitizer）
#   --user <U>      Nexus 用户名（也可用环境变量 NEXUS_USERNAME）
#   --pass <P>      Nexus 密码（也可用环境变量 NEXUS_PASSWORD）
#
# 注：MR 产物缓存到 /tmp/tdci-mr-<N>/，二次运行无需重新下载。
# ======================================================================

set -euo pipefail

# ── 常量 ─────────────────────────────────────────────────────────────────────
NEXUS_URL="https://nexus.tdengine.net"
NEXUS_REPO="tdtest"
FAILLOG_BASE="/data1/tdengine-ci/fail-logs"

# ── 参数默认值 ────────────────────────────────────────────────────────────────
MR_NUM=""
CASE_NAME=""
DEBUG_DIR=""
COMMUNITY_DIR=""
SANITIZER="n"
SANITIZER_EXPLICIT=0   # 用户是否显式传了 -s
CMD=""
NEXUS_USERNAME="${NEXUS_USERNAME:-}"
NEXUS_PASSWORD="${NEXUS_PASSWORD:-}"

usage() {
    cat <<'EOF'
用法:
  rerun.sh [选项] [测试命令]

示例:
  # 本地运行（自动检测 /data/tsdb/debug）
  ./rerun.sh "pytest cases/09-DataQuerying/01-Select/test_query_select_bugs.py"

  # 指定 MR 构建产物运行（需 Nexus 凭证）
  ./rerun.sh --mr 42 "pytest cases/09-DataQuerying/01-Select/test_query_select_bugs.py"

  # 复现 CI 失败用例（在 runner 机器上执行）
  ./rerun.sh --case n4-09-DataQuerying_01-Select_test_query_select_bugs

  # 用 MR 42 构建复现 CI 失败用例
  ./rerun.sh --mr 42 --case n4-09-DataQuerying_01-Select_test_query_select_bugs

选项:
  --mr  <N>       MR 号，从 Nexus 下载构建产物
  --case <name>   从 fail-logs 读取失败用例（runner 机器）
  -d <DIR>        手动指定 debug/debugNoSan 目录
  -r <DIR>        手动指定 taos-community 目录
  -s              使用 sanitizer 构建
  --user <U>      Nexus 用户名
  --pass <P>      Nexus 密码
EOF
}

# ── 参数解析 ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mr)
            [[ -z "${2:-}" ]] && { echo "ERROR: --mr 需要 MR 号"; exit 1; }
            MR_NUM="$2"; shift 2 ;;
        --case)
            [[ -z "${2:-}" ]] && { echo "ERROR: --case 需要用例名"; exit 1; }
            CASE_NAME="$2"; shift 2 ;;
        -d)
            [[ -z "${2:-}" ]] && { echo "ERROR: -d 需要目录路径"; exit 1; }
            DEBUG_DIR="$2"; shift 2 ;;
        -r)
            [[ -z "${2:-}" ]] && { echo "ERROR: -r 需要目录路径"; exit 1; }
            COMMUNITY_DIR="$2"; shift 2 ;;
        -s)
            SANITIZER="y"; SANITIZER_EXPLICIT=1; shift ;;
        --user)
            [[ -z "${2:-}" ]] && { echo "ERROR: --user 需要用户名"; exit 1; }
            NEXUS_USERNAME="$2"; shift 2 ;;
        --pass)
            [[ -z "${2:-}" ]] && { echo "ERROR: --pass 需要密码"; exit 1; }
            NEXUS_PASSWORD="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        -*)
            echo "ERROR: 未知选项: $1"; usage; exit 1 ;;
        *)
            if [[ -z "$CMD" ]]; then
                CMD="$1"; shift
            else
                echo "ERROR: 多余的参数: $1"; usage; exit 1
            fi ;;
    esac
done

if [[ -z "$CMD" && -z "$CASE_NAME" ]]; then
    echo "ERROR: 需要提供测试命令或 --case 选项"
    echo ""
    usage
    exit 1
fi

# ── 自动检测 taos-community 目录 ──────────────────────────────────────────────
if [[ -z "$COMMUNITY_DIR" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    # 本脚本在 <community>/test/ci/ 下
    CANDIDATE="$(cd "$SCRIPT_DIR/../.." && pwd)"
    if [[ -f "$CANDIDATE/cmake/version.cmake" ]]; then
        COMMUNITY_DIR="$CANDIDATE"
    else
        # 在 gitlab-runner builds 目录下扫描（runner 机器 checkout 结构）
        COMMUNITY_DIR=$(find /data0/gitlab-runner/builds -maxdepth 6 \
            -path "*/rd-public/tsdb/source/taos-community" -type d 2>/dev/null | head -1 || true)
    fi
fi
[[ -z "$COMMUNITY_DIR" ]] && {
    echo "ERROR: 找不到 taos-community 目录，请用 -r 指定"
    exit 1
}
[[ -d "$COMMUNITY_DIR" ]] || {
    echo "ERROR: community 目录不存在: $COMMUNITY_DIR"
    exit 1
}

# ── 处理 --case：从 fail-logs 提取信息 ───────────────────────────────────────
if [[ -n "$CASE_NAME" ]]; then
    CASE_TXT=""
    if [[ -f "$CASE_NAME" ]]; then
        CASE_TXT="$CASE_NAME"
    elif [[ -f "$FAILLOG_BASE/$CASE_NAME/case.txt" ]]; then
        # 旧结构: FAILLOG_BASE/<slug>/case.txt（兼容旧格式）
        CASE_TXT="$FAILLOG_BASE/$CASE_NAME/case.txt"
    else
        # 新结构: FAILLOG_BASE/job-<id>/<slug>/case.txt
        # 按 job-id 降序搜索（最新 job 优先）
        FOUND_CASE_TXT=$(find "$FAILLOG_BASE" -maxdepth 3 -name "case.txt" \
            -path "*/${CASE_NAME}/case.txt" 2>/dev/null \
            | sort -t/ -k5 -rV | head -1 || true)
        if [[ -n "$FOUND_CASE_TXT" ]]; then
            CASE_TXT="$FOUND_CASE_TXT"
        fi
    fi

    if [[ -z "$CASE_TXT" ]]; then
        echo "ERROR: 找不到 fail-log，检查用例名: $CASE_NAME"
        echo ""
        if [[ -d "$FAILLOG_BASE" ]]; then
            echo "已保留的失败 job:"
            ls "$FAILLOG_BASE/" | grep -v "^_" | sed 's/^/  /' || true
            echo ""
            echo "提示: 用例名格式为 n<NODE>-<path>，例如:"
            find "$FAILLOG_BASE" -maxdepth 3 -name "case.txt" 2>/dev/null \
                | sed 's|.*/\([^/]*\)/case\.txt|  \1|' | sort -u | head -10 || true
        else
            echo "  (fail-logs 目录不存在: $FAILLOG_BASE)"
            echo "  此功能仅在 runner 机器上可用"
        fi
        exit 1
    fi

    echo "读取失败日志: $CASE_TXT"

    # ── 解析 case.txt：支持新格式（KEY=VALUE）和旧格式（cp/docker run）────
    if grep -q '^COMMUNITY_DIR=' "$CASE_TXT" 2>/dev/null; then
        # 新格式（由 run-test-dynamic.sh 生成）
        _ct_community=$(grep '^COMMUNITY_DIR=' "$CASE_TXT" | cut -d= -f2-)
        _ct_debug=$(grep '^DEBUG_DIR=' "$CASE_TXT" | cut -d= -f2-)
        _ct_san=$(grep '^SANITIZER=' "$CASE_TXT" | cut -d= -f2-)
        _ct_cmd=$(grep '^CMD=' "$CASE_TXT" | cut -d= -f2-)

        [[ -d "$_ct_community" ]] && COMMUNITY_DIR="$_ct_community"
        if [[ -z "$DEBUG_DIR" && -z "$MR_NUM" && -d "$_ct_debug" ]]; then
            DEBUG_DIR="$_ct_debug"
            echo "INFO: 使用原始 job debug 目录: $DEBUG_DIR"
        fi
        if [[ -z "$CMD" ]]; then
            CMD="$_ct_cmd"
            [[ -z "$CMD" ]] && { echo "ERROR: case.txt 中无 CMD 字段"; exit 1; }
        fi
        if [[ $SANITIZER_EXPLICIT -eq 0 ]]; then
            if [[ "$_ct_san" == "y" ]]; then
                SANITIZER="y"
                echo "INFO: 原始用例为 ASAN 构建，自动启用 sanitizer 模式"
            else
                SANITIZER="n"
            fi
        fi
    else
        # 旧格式（cp -rf ... docker run ... 行）
        SOURCE_FROM_LOG=$(grep "^cp -rf" "$CASE_TXT" | head -1 | awk '{print $3}' | sed 's|/test/\*$||')
        DOCKER_CMD_FROM_LOG=$(grep "^docker run" "$CASE_TXT" | head -1)

        [[ -z "$SOURCE_FROM_LOG" ]] && { echo "ERROR: 无法从 case.txt 解析 source 目录"; exit 1; }
        [[ -z "$DOCKER_CMD_FROM_LOG" ]] && { echo "ERROR: 无法从 case.txt 解析 docker 命令"; exit 1; }

        [[ -d "$SOURCE_FROM_LOG" ]] && COMMUNITY_DIR="$SOURCE_FROM_LOG"

        if [[ -z "$CMD" ]]; then
            CMD=$(echo "$DOCKER_CMD_FROM_LOG" | grep -oP '(?<=-c ")[^"]+' || true)
            [[ -z "$CMD" ]] && CMD=$(echo "$DOCKER_CMD_FROM_LOG" | grep -oP '(?<=run_case\.sh ).*' | sed 's/ -e$//' || true)
            [[ -z "$CMD" ]] && { echo "ERROR: 无法从 case.txt 提取测试命令"; exit 1; }
        fi

        if [[ $SANITIZER_EXPLICIT -eq 0 ]]; then
            if echo "$DOCKER_CMD_FROM_LOG" | grep -q "CI_NO_ASAN=1"; then
                SANITIZER="n"
            else
                SANITIZER="y"
                echo "INFO: 原始用例使用 ASAN 构建，自动启用 sanitizer 模式"
            fi
        fi

        if [[ -z "$DEBUG_DIR" && -z "$MR_NUM" ]]; then
            ORIG_DEBUG=$(echo "$DOCKER_CMD_FROM_LOG" \
                | grep -oP '(?<=-v )/[^ ]+(?=/:/home/TDinternal/debug/)' | head -1 || true)
            [[ -n "$ORIG_DEBUG" && -d "$ORIG_DEBUG" ]] && {
                DEBUG_DIR="$ORIG_DEBUG"
                echo "INFO: 使用原始 job debug 目录: $DEBUG_DIR"
            }
        fi
    fi
fi

# ── 从 cases.task 推断 ASAN 模式（仅在无 --case 时使用，作为下载包的提示）────
# --case 模式已从 fail-log 的 docker 命令得到确切结果，不再用 cases.task 覆盖
if [[ $SANITIZER_EXPLICIT -eq 0 && -z "$CASE_NAME" && -n "$CMD" ]]; then
    TASK_FILE="${COMMUNITY_DIR}/test/ci/cases.task"
    if [[ -f "$TASK_FILE" ]]; then
        PY_FILE=$(echo "$CMD" | grep -oP '[^ ]+\.py' | head -1 || true)
        if [[ -n "$PY_FILE" ]]; then
            # cases.task 格式：priority,rerunTimes,sanitizer(y/n),casePath,caseCommand
            SAN=$(grep -v '^#' "$TASK_FILE" | grep -v '^[[:space:]]*$' | \
                  grep "$PY_FILE" | head -1 | cut -d',' -f3 | tr -d ' ' || true)
            if [[ "$SAN" == "y" ]]; then
                SANITIZER="y"
                echo "INFO: cases.task 中该用例为 ASAN 构建"
            elif [[ "$SAN" == "n" ]]; then
                SANITIZER="n"
            fi
        fi
    fi
fi

# ── 确定 debug 目录 ───────────────────────────────────────────────────────────
if [[ -n "$DEBUG_DIR" ]]; then
    # 手动指定，直接用
    :
elif [[ -n "$MR_NUM" ]]; then
    # 根据 ASAN 模式选择对应的 Nexus 包和缓存目录
    if [[ "$SANITIZER" == "y" ]]; then
        NEXUS_PATH="tsdb/ci/mr${MR_NUM}/linux/x64/asan"
        ARTIFACT_FILE="linux-x64-asan.tar.gz"
        DOWNLOAD_DIR="/tmp/tdci-mr-${MR_NUM}-asan"
    else
        NEXUS_PATH="tsdb/ci/mr${MR_NUM}/linux/x64/noasan"
        ARTIFACT_FILE="linux-x64-noasan.tar.gz"
        DOWNLOAD_DIR="/tmp/tdci-mr-${MR_NUM}-noasan"
    fi
    ARTIFACT_URL="${NEXUS_URL}/repository/${NEXUS_REPO}/${NEXUS_PATH}/${ARTIFACT_FILE}"

    if [[ -d "${DOWNLOAD_DIR}/debugNoSan/build/bin" ]]; then
        echo "INFO: 使用已缓存的 MR ${MR_NUM} 产物 ($( [[ "$SANITIZER" == "y" ]] && echo ASAN || echo non-ASAN )): ${DOWNLOAD_DIR}/debugNoSan"
        DEBUG_DIR="${DOWNLOAD_DIR}/debugNoSan"
    else
        echo "下载 MR ${MR_NUM} 构建产物 ($( [[ "$SANITIZER" == "y" ]] && echo ASAN || echo non-ASAN ))..."
        echo "  URL: $ARTIFACT_URL"
        echo "  目标: $DOWNLOAD_DIR"
        mkdir -p "$DOWNLOAD_DIR"

        # 构造认证参数（Nexus 公开 repo 无需认证，有凭证时加上）
        CURL_AUTH=()
        [[ -n "$NEXUS_USERNAME" && -n "$NEXUS_PASSWORD" ]] && \
            CURL_AUTH=(-u "${NEXUS_USERNAME}:${NEXUS_PASSWORD}")

        for attempt in 1 2 3; do
            echo "  下载尝试 ${attempt}/3..."
            if curl -fsSL \
                    --retry 2 --retry-delay 5 \
                    --connect-timeout 30 --max-time 300 \
                    "${CURL_AUTH[@]}" \
                    "${ARTIFACT_URL}" \
                    -o "${DOWNLOAD_DIR}/${ARTIFACT_FILE}"; then
                echo "  下载完成 ($(du -sh "${DOWNLOAD_DIR}/${ARTIFACT_FILE}" | cut -f1))"
                break
            fi
            if [[ $attempt -eq 3 ]]; then
                echo "ERROR: 下载失败（重试 3 次后放弃）"
                exit 1
            fi
            echo "  等待 10s 后重试..."
            sleep 10
        done

        echo "解压产物..."
        (cd "${DOWNLOAD_DIR}" && tar xzf "${ARTIFACT_FILE}")
        rm -f "${DOWNLOAD_DIR}/${ARTIFACT_FILE}"

        # debug/ → debugNoSan/（与 pull-artifacts.sh 保持一致）
        if [[ -d "${DOWNLOAD_DIR}/debug" && ! -d "${DOWNLOAD_DIR}/debugNoSan" ]]; then
            mv "${DOWNLOAD_DIR}/debug" "${DOWNLOAD_DIR}/debugNoSan"
        fi

        # 验证
        for f in taosd taos taosadapter; do
            BIN="${DOWNLOAD_DIR}/debugNoSan/build/bin/${f}"
            if [[ -f "$BIN" ]]; then
                echo "  [OK] ${f}: $(du -sh "$BIN" | cut -f1)"
            else
                echo "  [MISSING] ${f}"
            fi
        done

        DEBUG_DIR="${DOWNLOAD_DIR}/debugNoSan"
        echo "产物目录: $DEBUG_DIR"
    fi
elif [[ -d "/data/tsdb/debug" ]]; then
    # 开发机默认路径
    DEBUG_DIR="/data/tsdb/debug"
    echo "INFO: 使用开发机 debug 目录: $DEBUG_DIR"
else
    # runner 机器：寻找正在运行的最新 job 的 debug 目录
    DEBUG_DIR=$(find /data1/tdengine-ci -maxdepth 2 -name "debugNoSan" -type d 2>/dev/null \
                | sort | tail -1 || true)
    if [[ -z "$DEBUG_DIR" ]]; then
        echo "ERROR: 找不到可用的 debug 目录"
        echo "  选项："
        echo "    开发机：默认使用 /data/tsdb/debug（cmake 构建后）"
        echo "    runner：等待 CI job 开始后自动检测，或用 -d 指定"
        echo "    任意：用 --mr <N> 从 Nexus 下载 MR 的构建产物"
        exit 1
    fi
    echo "INFO: 自动选用 runner debug 目录: $DEBUG_DIR"
fi

[[ -d "$DEBUG_DIR" ]] || { echo "ERROR: debug 目录不存在: $DEBUG_DIR"; exit 1; }

# ── 准备临时目录 ──────────────────────────────────────────────────────────────
TMP_DIR="/tmp/tdci-run-$$"
mkdir -p "$TMP_DIR/sim/var_taoslog" "$TMP_DIR/sim/tsim" "$TMP_DIR/sim/asan" "$TMP_DIR/coredump"

echo "复制测试文件: ${COMMUNITY_DIR}/test/ → ${TMP_DIR}/"
cp -rf "${COMMUNITY_DIR}/test/"* "$TMP_DIR/"

ASAN_ENV=""
[[ "$SANITIZER" != "y" ]] && ASAN_ENV="-e CI_NO_ASAN=1"

# ── 输出摘要 ──────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Community : $COMMUNITY_DIR"
echo "  Debug     : $DEBUG_DIR"
echo "  Command   : $CMD"
echo "  Sanitizer : $SANITIZER"
[[ -n "$MR_NUM" ]]    && echo "  MR        : #${MR_NUM}"
[[ -n "$CASE_NAME" ]] && echo "  Case      : ${CASE_NAME}"
echo "  Logs      : $TMP_DIR/sim/dnode1/log/taosdlog.0"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── 运行容器 ──────────────────────────────────────────────────────────────────
# shellcheck disable=SC2086
docker run --rm --privileged=true \
    $ASAN_ENV \
    --name "tdci-run-$$" \
    -v "${COMMUNITY_DIR}:/home/TDinternal/community" \
    -v "${DEBUG_DIR}:/home/TDinternal/debug/" \
    -v "${DEBUG_DIR}/build/lib:/home/TDinternal/debug/build/lib:ro" \
    -v "${TMP_DIR}:/home/TDinternal/community/test" \
    -v "${TMP_DIR}/sim:/home/TDinternal/sim" \
    -v "${TMP_DIR}/coredump:/tmp" \
    --ulimit core=-1 \
    tdengine-ci:0.1 \
    /home/TDinternal/community/test/ci/run_case.sh -d "." -c "$CMD" -e
ret=$?

# ── 输出结果 ──────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ $ret -eq 0 ]]; then
    echo "  结果: PASSED"
    echo "  临时目录: rm -rf $TMP_DIR"
else
    echo "  结果: FAILED (exit=$ret)"
    echo "  taosd 日志: cat $TMP_DIR/sim/dnode1/log/taosdlog.0"
    echo "  完整 sim  : ls $TMP_DIR/sim/"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
exit $ret
