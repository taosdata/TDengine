#!/bin/bash
# Usage: compat_publish_main.sh <target_pkg_path> <green_base_dir> [branch]
#
# <target_pkg_path> : full path to the newly published .tar.gz package
# <green_base_dir>  : directory containing green baseline version subdirs
#                     (e.g. /green_version/3.3.6.0/{taosd,libtaos.so})
# [branch]          : target branch name, default: main
#                     supported: main, 3.3.6

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXTRACT_SCRIPT="$SCRIPT_DIR/extract_pkg.sh"
COMPAT_CHECK_DIR="$(cd "$SCRIPT_DIR/../compat-check" && pwd)"

# Baseline versions for branch: main (default)
BASELINE_VERSIONS_MAIN=(
    3.0.7.2
    3.1.1.7
    3.2.0.0
    3.2.1.0
    3.3.5.0
    3.3.6.0
    3.3.8.22
    3.4.0.0
    3.4.0.14
)

# Baseline versions for branch: 3.3.6
BASELINE_VERSIONS_336=(
    3.1.0.0
    3.2.0.0
    3.3.5.0
    3.3.6.0
    3.3.6.20
    3.3.6.31
)

# ── argument check ────────────────────────────────────────────────────────────
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <target_pkg_path> <green_base_dir> [branch]"
    echo "  branch: main (default) | 3.3.6"
    exit 1
fi

TARGET_PKG="$1"
GREEN_BASE_DIR="${2%/}"
BRANCH="${3:-main}"

case "$BRANCH" in
    main)   BASELINE_VERSIONS=("${BASELINE_VERSIONS_MAIN[@]}") ;;
    3.3.6)  BASELINE_VERSIONS=("${BASELINE_VERSIONS_336[@]}") ;;
    *)
        echo "Error: unknown branch '${BRANCH}'. Supported: main, 3.3.6"
        exit 1
        ;;
esac

if [ ! -f "$TARGET_PKG" ]; then
    echo "Error: target package not found: $TARGET_PKG"
    exit 1
fi

if [ ! -d "$GREEN_BASE_DIR" ]; then
    echo "Error: green base directory not found: $GREEN_BASE_DIR"
    exit 1
fi

if [ ! -f "$EXTRACT_SCRIPT" ]; then
    echo "Error: extract_pkg.sh not found: $EXTRACT_SCRIPT"
    exit 1
fi


# ── validate package name ─────────────────────────────────────────────────────
PKG_BASENAME=$(basename "$TARGET_PKG")
# Reject known non-server packages: explorer, client, agent, docker, debug, tdgpt
if echo "$PKG_BASENAME" | grep -qiE 'explorer|client|agent|docker|debug|tdgpt'; then
    echo "Error: wrong package type: $PKG_BASENAME"
    echo ""
    echo "  This script requires a TDengine enterprise SERVER package, which contains"
    echo "  taosd and libtaos.so. Common server package name patterns:"
    echo "    TDengine-enterprise-server-<ver>-Linux-x64.tar.gz     (3.0.x ~ 3.1.x)"
    echo "    TDengine-enterprise-<ver>-Linux-x64.tar.gz            (3.2.x ~ 3.3.5.x)"
    echo "    tdengine-tsdb-enterprise-<ver>-linux-x64.tar.gz       (3.3.6.x and later)"
    echo ""
    echo "  The package you provided looks like a non-server component:"
    echo "    $PKG_BASENAME"
    exit 1
fi

# ── step 1: extract target green files into ./target ─────────────────────────
TARGET_DIR="$SCRIPT_DIR/target"
mkdir -p "$TARGET_DIR"

echo "===== Extracting target package ====="
echo "  Package : $TARGET_PKG"
echo "  Into    : $TARGET_DIR"
bash "$EXTRACT_SCRIPT" "$TARGET_PKG" "$TARGET_DIR"

# figure out the version that was just extracted (the only subdir or the newest)
TARGET_VER_DIR=$(ls -td "$TARGET_DIR"/[0-9]*.[0-9]*.[0-9]*.[0-9]* 2>/dev/null | head -1)
if [ -z "$TARGET_VER_DIR" ]; then
    echo "Error: could not find extracted version subdir under $TARGET_DIR"
    exit 1
fi
TARGET_VERSION=$(basename "$TARGET_VER_DIR")
echo "  Target version : $TARGET_VERSION"
echo ""

# ── step 2: run compat-check for each baseline version ───────────────────────
PASS_LIST=()
FAIL_LIST=()

for base_ver in "${BASELINE_VERSIONS[@]}"; do
    FROM_DIR="$GREEN_BASE_DIR/$base_ver"
    TO_DIR="$TARGET_VER_DIR"

    echo "===== compat-check: $base_ver -> $TARGET_VERSION ====="

    if [ ! -d "$FROM_DIR" ]; then
        echo "  SKIP: baseline dir not found: $FROM_DIR"
        FAIL_LIST+=("$base_ver (baseline dir missing)")
        continue
    fi

    WORK_DIR="$SCRIPT_DIR/data/${base_ver}_${TARGET_VERSION}"
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"

    pkill -9 taosd 2>/dev/null || true

    # versions before 3.4.0.0: skip rsma/tsma/user/index checks
    EXTRA_FLAGS=""
    if printf '%s\n%s\n' "3.4.0.0" "$base_ver" | sort -V | head -1 | grep -qx "$base_ver"; then
        if [ "$base_ver" != "3.4.0.0" ]; then
            EXTRA_FLAGS="--no-rsma --no-tsma --no-user --no-index"
        fi
    fi

    LOG="$WORK_DIR/compat_test.log"
    CMD_ARGS=(python3 run/main.py
        --from-dir "$FROM_DIR"
        --to-dir   "$TO_DIR"
        --path     "$WORK_DIR"
    )
    [ -n "$EXTRA_FLAGS" ] && CMD_ARGS+=($EXTRA_FLAGS)
    echo "  CMD: cd \"$COMPAT_CHECK_DIR\" && ${CMD_ARGS[*]}"
    (
        cd "$COMPAT_CHECK_DIR"
        "${CMD_ARGS[@]}"
    ) 2>&1 | tee "$LOG"
    # capture python exit code, not tee's
    RET=${PIPESTATUS[0]}

    if [ "$RET" -eq 0 ]; then
        echo "  RESULT: PASS"
        PASS_LIST+=("$base_ver")
    else
        echo "  RESULT: FAIL (exit $RET)"
        FAIL_LIST+=("$base_ver")
    fi

    echo ""
done

# ── step 3: summary ──────────────────────────────────────────────────────────
echo "============================================================"
echo "  Compat-check summary  (base -> $TARGET_VERSION)"
echo "============================================================"
echo "  PASS (${#PASS_LIST[@]}):"
for v in "${PASS_LIST[@]}"; do echo "    $v"; done

echo "  FAIL (${#FAIL_LIST[@]}):"
for v in "${FAIL_LIST[@]}"; do echo "    $v"; done
echo "============================================================"

if [ ${#FAIL_LIST[@]} -gt 0 ]; then
    exit 1
fi
