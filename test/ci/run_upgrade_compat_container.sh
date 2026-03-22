#!/bin/bash
# run_upgrade_compat_container.sh
#
# 冷/热升级兼容性测试执行脚本（Docker 容器内侧）
#
# 本脚本由 run_upgrade_compat.sh 通过 docker run 调用，在容器内执行：
#   1. 安装 Python 依赖（taospy / pyyaml）
#   2. 冷升级测试：3.3.6.0 / 3.3.8.0 / 3.4.0.0 → 当前版本（一次调用，串行）
#   3. 热升级（滚动升级）测试：自动匹配同版本前缀
#   4. 汇总输出 PASS / FAIL
#
# 容器内挂载约定（由 run_upgrade_compat.sh 保证）：
#   /home/TDinternal          — 企业版源码（只读）
#   /home/TDinternal/debug    — 当前版本编译产物 debugNoSan（只读）
#   /green_versions           — 绿色版本缓存目录（只读）
#   /upgrade_logs             — 日志输出目录（可写）

# 
#  cold upgrade version (2 group)
#

# group1
COLD_VERSIONS_1="3.3.6.0,3.3.8.0"
COLD_VERSIONS_OPTION_1="--no-tsma --no-user"

# group2
COLD_VERSIONS_2="3.4.0.0"
COLD_VERSIONS_OPTION_2=""


# ── 路径配置 ─────────────────────────────────────────────────────────────────

GREEN_PATH="/green_versions"
LOG_DIR="/upgrade_logs"
COMPAT_CI_DIR="/home/TDinternal/community/test/tools/CompatCI"

if [ -z "$COLD_VERSIONS_CSV" ]; then
    echo "ERROR: -V COLD_VERSIONS_CSV is required"
    usage
    exit 1
fi

mkdir -p "$LOG_DIR"

# ── 前置检查 ─────────────────────────────────────────────────────────────────

if [ ! -d "$COMPAT_CI_DIR" ]; then
    echo "ERROR: CompatCI directory not found: $COMPAT_CI_DIR"
    exit 1
fi

if [ ! -d "$GREEN_PATH" ] || [ -z "$(ls -A "$GREEN_PATH" 2>/dev/null)" ]; then
    echo "ERROR: Green versions directory is empty or missing: $GREEN_PATH"
    exit 1
fi

echo "======================================================"
echo "  Upgrade Compatibility Test (container)"
echo "======================================================"
echo "  COMPAT_CI_DIR : $COMPAT_CI_DIR"
echo "  GREEN_PATH    : $GREEN_PATH"
echo "  LOG_DIR       : $LOG_DIR"
echo "  Enterprise    : $ent"
echo "  Green versions available:"
ls -1 "$GREEN_PATH" | sed 's/^/    /'
echo "======================================================"

# ── Step 0: 安装 Python 依赖 ─────────────────────────────────────────────────

echo ""
echo "=== Installing Python dependencies ==="
pip3 install taospy pyyaml -q
echo "=== Python dependencies ready ==="

# ── 工具函数：清理 taosd 进程 ────────────────────────────────────────────────

function cleanup_taosd() {
    pkill -TERM -f "taosd" 2>/dev/null || true
    sleep 2
    pkill -KILL -f "taosd" 2>/dev/null || true
}

overall_ret=0

# ── Step 1: 冷升级测试（3.3.6.0 / 3.3.8.0 / 3.4.0.0 → 当前版本，一次调用）──

echo ""
echo "============================================================"
echo "=== Cold Upgrade Tests (3.3.6.0, 3.3.8.0, 3.4.0.0)     ==="
echo "============================================================"

#
# 3.3.x.x
#

cleanup_taosd

cold_cmd=(python3 "$COMPAT_CI_DIR/cold_upgrade_task.py"
    --green-path "$GREEN_PATH"
    --versions   "$COLD_VERSIONS_1"
    --options="$COLD_VERSIONS_OPTION_1"
)
echo "Executing: ${cold_cmd[*]}"
"${cold_cmd[@]}" 2>&1 | tee "$LOG_DIR/cold_upgrade.log"

cold_ret=${PIPESTATUS[0]}

if [ $cold_ret -ne 0 ]; then
    echo "[FAIL] Cold upgrade 3.3.x.x tests FAILED (exit code: $cold_ret)"
    overall_ret=$cold_ret
fi

#
# 3.4.x.x
#

cleanup_taosd

cold_cmd=(python3 "$COMPAT_CI_DIR/cold_upgrade_task.py"
    --green-path "$GREEN_PATH"
    --versions   "$COLD_VERSIONS_2"
    --options="$COLD_VERSIONS_OPTION_2"
)
echo "Executing: ${cold_cmd[*]}"
"${cold_cmd[@]}" 2>&1 | tee "$LOG_DIR/cold_upgrade.log"

cold_ret=${PIPESTATUS[0]}

if [ $cold_ret -ne 0 ]; then
    echo "[FAIL] Cold upgrade 3.4.x.x tests FAILED (exit code: $cold_ret)"
    overall_ret=$cold_ret
else
    echo "[PASS] Cold upgrade tests PASSED"
fi


# ── Step 2: 热升级（滚动升级）测试 ──────────────────────────────────────────

echo ""
echo "============================================================"
echo "=== Hot (Rolling) Upgrade Test                          ==="
echo "============================================================"

# 冷升级结束后 taosd 仍在运行，清理后再做热升级
cleanup_taosd

hot_cmd=(python3 "$COMPAT_CI_DIR/hot_upgrade_task.py"
    --green-path "$GREEN_PATH"
    -q
)
echo "Executing: ${hot_cmd[*]}"
"${hot_cmd[@]}" 2>&1 | tee "$LOG_DIR/hot_upgrade.log"

hot_ret=${PIPESTATUS[0]}

# hot_upgrade_task 在当前版本无匹配绿色版本时返回 0 并输出 "No rolling upgrade test needed"
# 此为正常跳过，不计入失败
if [ $hot_ret -ne 0 ]; then
    echo "[FAIL] Hot upgrade test FAILED (exit code: $hot_ret)"
    overall_ret=$hot_ret
else
    if grep -q "No rolling upgrade test needed" "$LOG_DIR/hot_upgrade.log" 2>/dev/null; then
        echo "[SKIP] Hot upgrade test SKIPPED (no matching base version for current build)"
    else
        echo "[PASS] Hot upgrade test PASSED"
    fi
fi

# ── 汇总 ─────────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "=== Upgrade Compatibility Test Summary                  ==="
echo "============================================================"

# 从冷升级日志中提取各版本结果
for ver in 3.3.6.0 3.3.8.0 3.4.0.0; do
    if grep -q "\[PASS\] Cold upgrade test passed.*${ver}" "$LOG_DIR/cold_upgrade.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : PASS"
    elif grep -q "\[FAIL\] Cold upgrade test FAILED.*${ver}" "$LOG_DIR/cold_upgrade.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : FAIL"
    else
        echo "  Cold ${ver} → current : UNKNOWN"
    fi
done

if grep -q "No rolling upgrade test needed" "$LOG_DIR/hot_upgrade.log" 2>/dev/null; then
    echo "  Hot  (rolling)       : SKIP"
elif [ $hot_ret -eq 0 ]; then
    echo "  Hot  (rolling)       : PASS"
else
    echo "  Hot  (rolling)       : FAIL"
fi

echo "============================================================"

if [ $overall_ret -eq 0 ]; then
    echo "  Overall Result : PASS ✓"
else
    echo "  Overall Result : FAIL ✗ (exit code: $overall_ret)"
fi

echo "============================================================"
echo "  Logs: $LOG_DIR"
echo "============================================================"

exit $overall_ret
