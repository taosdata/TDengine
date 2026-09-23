#!/bin/bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# ── Configuration ─────────────────────────────────────────────────────────────

# 
#  cold upgrade version (2 group)
#

# group1
COLD_VERSIONS_1="3.3.6.0,3.3.8.0"
COLD_VERSIONS_OPTION_1="--no-tsma --no-user"

# group2
COLD_VERSIONS_2="3.4.0.0,3.4.1.0"
COLD_VERSIONS_OPTION_2="--check-sysinfo"


# ── Paths ─────────────────────────────────────────────────────────────────────

GREEN_PATH="/green_versions"
LOG_DIR="/upgrade_logs"
BUILD_DIR="/home/TDinternal/debug"
COMPAT_CI_DIR="/home/TDinternal/community/test/tools/compat-ci"


mkdir -p "$LOG_DIR"

# ── Pre-activation checks ─────────────────────────────────────────────────────────

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

# ── Step 0.5: Clear the "requires executable stack" ELF flag on libtaos.so ───
# The prebuilt green-version client libraries (downloaded from the NAS) were
# built with an old toolchain that marks libtaos.so as requiring an executable
# stack (PT_GNU_STACK + PF_X). Modern kernels enforce DEP/NX for the stack
# regardless of container privilege (--privileged does NOT bypass this — it's
# not a seccomp/capability restriction), so ctypes.CDLL("libtaos.so") fails
# with "cannot enable executable stack as shared object requires: Invalid
# argument". The usual fix is `execstack -c`, but that tool isn't installed in
# the test image and apt-get has no network access in this container — so we
# patch the PT_GNU_STACK program header's PF_X bit directly with a small,
# dependency-free Python script instead. Harmless: these client libraries
# don't actually rely on trampoline code needing an executable stack. Fixed
# Green-version libraries are patched before they are published to the shared
# cache. Only the current build directory needs patching in this container.
echo ""
echo "=== Clearing executable-stack flag on current libtaos.so binaries ==="
python3 - "$BUILD_DIR" <<'PYEOF'
import glob
import os
import struct
import sys

PT_GNU_STACK = 0x6474e551
PF_X = 0x1


def clear_exec_stack(path):
    with open(path, "rb") as f:
        data = bytearray(f.read())
    if data[:4] != b"\x7fELF":
        return False
    ei_class = data[4]      # 1 = 32-bit, 2 = 64-bit
    ei_data = data[5]       # 1 = little-endian, 2 = big-endian
    endian = "<" if ei_data == 1 else ">"
    if ei_class == 2:
        phoff, = struct.unpack_from(endian + "Q", data, 0x20)
        phentsize, = struct.unpack_from(endian + "H", data, 0x36)
        phnum, = struct.unpack_from(endian + "H", data, 0x38)
        flags_off = 4
    elif ei_class == 1:
        phoff, = struct.unpack_from(endian + "I", data, 0x1C)
        phentsize, = struct.unpack_from(endian + "H", data, 0x2A)
        phnum, = struct.unpack_from(endian + "H", data, 0x2C)
        flags_off = 24
    else:
        return False

    changed = False
    for i in range(phnum):
        off = phoff + i * phentsize
        p_type, = struct.unpack_from(endian + "I", data, off)
        if p_type != PT_GNU_STACK:
            continue
        p_flags, = struct.unpack_from(endian + "I", data, off + flags_off)
        if p_flags & PF_X:
            struct.pack_into(endian + "I", data, off + flags_off, p_flags & ~PF_X)
            changed = True

    if changed:
        with open(path, "r+b") as f:
            f.seek(0)
            f.write(data)
    return changed


roots = [r for r in sys.argv[1:] if r and os.path.isdir(r)]
seen = set()
for root in roots:
    for so in glob.glob(os.path.join(root, "**", "libtaos*.so*"), recursive=True):
        real = os.path.realpath(so)
        if real in seen or not os.path.isfile(real):
            continue
        seen.add(real)
        try:
            if clear_exec_stack(real):
                print(f"  [OK] cleared exec-stack: {real}")
        except Exception as e:
            print(f"  [WARN] failed to patch {real}: {e}")
PYEOF

# ── Helper: kill taosd processes ─────────────────────────────────────────────

function cleanup_taosd() {
    pkill -TERM -f "taosd" 2>/dev/null || true
    sleep 2
    pkill -KILL -f "taosd" 2>/dev/null || true
}

overall_ret=0

# ── Step 1: Cold upgrade tests (3.3.6.0 / 3.3.8.0 / 3.4.0.0 → current, sequential) ──

echo ""
echo "============================================================"
echo "=== Cold Upgrade Tests (3.3.6.0, 3.3.8.0, 3.4.0.0)     ==="
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
for ver in 3.3.6.0 3.3.8.0; do
    if grep -q "\[PASS\] Cold upgrade test passed.*${ver}" "$LOG_DIR/cold_upgrade_33x.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : PASS"
    elif grep -q "\[FAIL\] Cold upgrade test FAILED.*${ver}" "$LOG_DIR/cold_upgrade_33x.log" 2>/dev/null; then
        echo "  Cold ${ver} → current : FAIL"
    else
        echo "  Cold ${ver} → current : UNKNOWN"
    fi
done
for ver in 3.4.0.0 3.4.1.0; do
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
