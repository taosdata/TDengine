#!/bin/bash
# run_upgrade_compat_container.sh
#
# Container-side script for cold/hot upgrade compatibility tests.
# Invoked by run_upgrade_compat.sh via docker run.
#
# Steps:
#   1. Install Python dependencies (taospy / pyyaml)
#   2. Cold upgrade tests: 3.3.6.0 / 3.3.8.0 / 3.4.0.0 → current version (sequential)
#   3. Hot (rolling) upgrade test: auto-match base version by version prefix
#   4. Print overall PASS / FAIL summary
#
# Volume mounts (guaranteed by run_upgrade_compat.sh):
#   /home/TDinternal          — source repo (read-only)
#   /home/TDinternal/debug    — current build artifacts debugNoSan (read-only)
#   /green_versions           — green version cache (read-only)
#   /upgrade_logs             — log output directory (writable)

# ── Configuration ─────────────────────────────────────────────────────────────

# 
#  cold upgrade version (2 group)
#

# group1
COLD_VERSIONS_1="3.1.0.0,3.2.0.0"
COLD_VERSIONS_OPTION_1="--no-tsma --no-user"

# group2
COLD_VERSIONS_2="3.3.5.0"
COLD_VERSIONS_OPTION_2="--no-tsma --no-user"


# ── Paths ─────────────────────────────────────────────────────────────────────

GREEN_PATH="/green_versions"
LOG_DIR="/upgrade_logs"
BUILD_DIR="/home/TDinternal/debug"
COMPAT_CI_DIR="/home/TDinternal/community/test/tools/compat-ci"


mkdir -p "$LOG_DIR"

# ── Pre-flight checks ─────────────────────────────────────────────────────────

if [ ! -d "$COMPAT_CI_DIR" ]; then
    echo "ERROR: compat-ci directory not found: $COMPAT_CI_DIR"
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
echo "  BUILD_DIR     : $BUILD_DIR"
echo "  LOG_DIR       : $LOG_DIR"
echo "======================================================"

# ── Step 0: Install Python dependencies ──────────────────────────────────────

echo ""
echo "=== Installing Python dependencies ==="
pip3 install taospy pyyaml -q --disable-pip-version-check 2>/dev/null
echo "=== Python dependencies installed successfully ==="

# ── Helper: kill taosd processes ─────────────────────────────────────────────

function cleanup_taosd() {
    pkill -TERM -f "taosd" 2>/dev/null || true
    sleep 2
    pkill -KILL -f "taosd" 2>/dev/null || true
}

overall_ret=0

# ── Step 1: Cold upgrade tests (3.3.x.x → current, sequential) ──

echo ""
echo "============================================================"
echo "=== Cold Upgrade Tests ==="
echo "============================================================"

#
# 3.3.6.0,3.3.8.0
#

cleanup_taosd

cold_cmd=(python3 "$COMPAT_CI_DIR/cold_upgrade_task.py"
    --green-path "$GREEN_PATH"
    --build-dir  "$BUILD_DIR"
    --versions   "$COLD_VERSIONS_1"
    --options="$COLD_VERSIONS_OPTION_1"
)
echo "Executing: ${cold_cmd[*]}"
"${cold_cmd[@]}" 2>&1 | tee "$LOG_DIR/cold_upgrade_33x.log"

cold_ret=${PIPESTATUS[0]}

if [ $cold_ret -ne 0 ]; then
    echo "[FAIL] Cold upgrade 3.3.x.x tests FAILED (exit code: $cold_ret)"
    overall_ret=$cold_ret
fi

#
# 3.4.0.0
#

cleanup_taosd

cold_cmd=(python3 "$COMPAT_CI_DIR/cold_upgrade_task.py"
    --green-path "$GREEN_PATH"
    --build-dir  "$BUILD_DIR"
    --versions   "$COLD_VERSIONS_2"
    --options="$COLD_VERSIONS_OPTION_2"
)
echo "Executing: ${cold_cmd[*]}"
"${cold_cmd[@]}" 2>&1 | tee "$LOG_DIR/cold_upgrade_34x.log"

cold_ret=${PIPESTATUS[0]}

if [ $cold_ret -ne 0 ]; then
    echo "[FAIL] Cold upgrade 3.4.0.0 tests FAILED (exit code: $cold_ret)"
    overall_ret=$cold_ret
else
    echo "[PASS] Cold upgrade tests PASSED"
fi


# ── Step 2: Hot (rolling) upgrade test ───────────────────────────────────────

echo ""
echo "============================================================"
echo "=== Hot (Rolling) Upgrade Test                          ==="
echo "============================================================"

# Clean up any lingering taosd before hot upgrade
cleanup_taosd

hot_cmd=(python3 "$COMPAT_CI_DIR/hot_upgrade_task.py"
    --green-path "$GREEN_PATH"
    --build-dir  "$BUILD_DIR"
    -q
)
echo "Executing: ${hot_cmd[*]}"
"${hot_cmd[@]}" 2>&1 | tee "$LOG_DIR/hot_upgrade.log"

hot_ret=${PIPESTATUS[0]}

# hot_upgrade_task exits 0 with "No rolling upgrade test needed" when no matching
# base version exists — treat this as a normal skip, not a failure
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

# ── Summary ───────────────────────────────────────────────────────────────────

echo ""
echo "============================================================"
echo "=== Upgrade Compatibility Test Summary                  ==="
echo "============================================================"

# Extract per-version results from cold upgrade logs
IFS=',' read -ra _vers1 <<< "$COLD_VERSIONS_1"
for ver in "${_vers1[@]}"; do
    if grep -q "\[PASS\] Cold upgrade test passed.*${ver}" "$LOG_DIR/cold_upgrade_33x.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : PASS"
    elif grep -q "\[FAIL\] Cold upgrade test FAILED.*${ver}" "$LOG_DIR/cold_upgrade_33x.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : FAIL"
    else
        echo "  Cold ${ver} → current : UNKNOWN"
    fi
done
IFS=',' read -ra _vers2 <<< "$COLD_VERSIONS_2"
for ver in "${_vers2[@]}"; do
    if grep -q "\[PASS\] Cold upgrade test passed.*${ver}" "$LOG_DIR/cold_upgrade_34x.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : PASS"
    elif grep -q "\[FAIL\] Cold upgrade test FAILED.*${ver}" "$LOG_DIR/cold_upgrade_34x.log" 2>/dev/null; then
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
