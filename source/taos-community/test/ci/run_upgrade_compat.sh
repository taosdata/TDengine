#!/bin/bash
# run_upgrade_compat.sh
#
# Host-side entry script for cold/hot upgrade compatibility tests.
#
# Steps:
#   1. Download required green versions from HTTP server to local cache (skipped if already cached)
#   2. Run cold/hot upgrade compatibility tests inside an isolated Docker container
#   3. Print test result logs
#
# Usage:
#   ./run_upgrade_compat.sh -w WORKDIR [-l LOG_DIR] [-h]
#
# Options:
#   -w WORKDIR   Working directory (contains TDinternal/ and debugNoSan/)
#   -l LOG_DIR   Log output directory (default: WORKDIR/upgrade_compat_logs)
#   -h           Show help

function usage() {
    echo "Usage: $0 -w WORKDIR [-l LOG_DIR] [-h]"
    echo ""
    echo "  -w WORKDIR   Working directory (contains TDinternal/ and debugNoSan/)"
    echo "  -l LOG_DIR   Log output directory (default: WORKDIR/upgrade_compat_logs)"
    echo "  -h           Show help"
}

# ── Configuration ─────────────────────────────────────────────────────────────

# Base URL of the green versions HTTP server
GREEN_HTTP_BASE="http://192.168.1.131/data/nas/TDengine/green_versions"

# Local cache directory for green versions (on host)
GREEN_LOCAL_DIR="/var/lib/jenkins/workspace/green_versions"

# Lock directory for concurrent download safety
LOCK_DIR="/tmp/green_versions_locks"


# ── Argument parsing ──────────────────────────────────────────────────────────

WORKDIR=""
LOG_DIR=""

while getopts "w:l:h" opt; do
    case $opt in
        w) WORKDIR=$OPTARG ;;
        l) LOG_DIR=$OPTARG ;;
        h) usage; exit 0 ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$WORKDIR" ]; then
    echo "ERROR: -w WORKDIR is required"
    usage
    exit 1
fi

if [ ! -d "$WORKDIR" ]; then
    echo "ERROR: WORKDIR does not exist: $WORKDIR"
    exit 1
fi

if [ -z "$LOG_DIR" ]; then
    LOG_DIR="$WORKDIR/upgrade_compat_logs"
fi

INTERNAL_REPDIR="$WORKDIR/TDinternal"
DEBUGPATH_DIR="$WORKDIR/debugNoSan"
CONTAINER_REP_MOUNT="$INTERNAL_REPDIR:/home/TDinternal"
CONTAINER_DEBUG_MOUNT="$DEBUGPATH_DIR:/home/TDinternal/debug"
CONTAINER_SCRIPT="/home/TDinternal/community/test/ci/run_upgrade_compat_container.sh"

mkdir -p "$LOG_DIR"
mkdir -p "$GREEN_LOCAL_DIR"
mkdir -p "$LOCK_DIR"

echo "======================================================"
echo "  Upgrade Compatibility Test"
echo "======================================================"
echo "  WORKDIR         : $WORKDIR"
echo "  LOG_DIR         : $LOG_DIR"
echo "  GREEN_LOCAL_DIR : $GREEN_LOCAL_DIR"
echo "======================================================"

# ── Step 1: Download green versions (with cache) ──────────────────────────────

function download_version() {
    local version="$1"
    local target_dir="$GREEN_LOCAL_DIR/$version"
    local lock_file="$LOCK_DIR/${version}.lock"
    local start_time
    start_time=$(date +%s)

    # Quick check: skip if cache exists and all files are >= 10M
    local file_count valid_count
    file_count=$(find "$target_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
    valid_count=$(find "$target_dir" -maxdepth 1 -type f -size +10M 2>/dev/null | wc -l)
    if [ "$file_count" -ge 2 ] && [ "$valid_count" -eq "$file_count" ]; then
        return 0
    fi

    # Use flock to prevent concurrent downloads of the same version
    (
        flock -x 200

        # Re-check inside lock in case another process already finished
        file_count=$(find "$target_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
        valid_count=$(find "$target_dir" -maxdepth 1 -type f -size +10M 2>/dev/null | wc -l)
        if [ "$file_count" -ge 2 ] && [ "$valid_count" -eq "$file_count" ]; then
            return 0
        fi

        echo "[green_versions] $version: downloading from $GREEN_HTTP_BASE/$version/ ..."
        mkdir -p "$target_dir"

        # Parse HTTP directory listing to get file names
        local file_list
        file_list=$(curl -fsSL "$GREEN_HTTP_BASE/$version/" \
            | sed -n 's/.*href="\([^"]*\)".*/\1/p' \
            | grep -v '/$\|^\.\.$\|^\.$\|^http\|^/' \
            | grep -v '?' \
            | sort -u)

        if [ -z "$file_list" ]; then
            echo "[green_versions] ERROR: $version: failed to list files from $GREEN_HTTP_BASE/$version/"
            rm -rf "$target_dir"
            return 1
        fi

        echo "[green_versions] $version: files to download: $(echo "$file_list" | tr '\n' ' ')"

        local failed=0
        for fname in $file_list; do
            local url="$GREEN_HTTP_BASE/$version/$fname"
            local dest="$target_dir/$fname"
            echo "[green_versions] $version: downloading $fname ..."
            if ! curl -fsSL "$url" -o "$dest"; then
                echo "[green_versions] ERROR: $version: failed to download $fname from $url"
                failed=1
                break
            fi
            # Make binaries executable
            chmod +x "$dest"
        done

        if [ $failed -ne 0 ]; then
            echo "[green_versions] $version: download failed, removing partial directory"
            rm -rf "$target_dir"
            return 1
        fi

        local end_time
        end_time=$(date +%s)
        echo "[green_versions] $version: download completed in $((end_time - start_time))s → $target_dir"

    ) 200>"$lock_file"

    return $?
}

echo ""
echo "=== Step 1/2: Downloading green versions ==="

# List all available version directories from the HTTP server
ALL_VERSIONS=$(curl -fsSL "$GREEN_HTTP_BASE/" \
    | sed -n 's/.*href="\([^"]*\)".*/\1/p' \
    | grep '/$' \
    | grep -v '^\.\.$\|^\.$\|^http\|^/' \
    | sed 's|/$||' \
    | sort -u)

if [ -z "$ALL_VERSIONS" ]; then
    echo "ERROR: Failed to list versions from $GREEN_HTTP_BASE/"
    exit 1
fi

echo "Available versions on server: $(echo "$ALL_VERSIONS" | tr '\n' ' ')"

download_failed=0
for ver in $ALL_VERSIONS; do
    download_version "$ver" || {
        echo "ERROR: Failed to download green version $ver"
        download_failed=1
    }
done

if [ $download_failed -ne 0 ]; then
    echo "ERROR: One or more green version downloads failed, aborting."
    exit 1
fi
echo "=== Step 1/2: All green versions ready ==="

# ── Step 2: Resolve paths ─────────────────────────────────────────────────────

if [ ! -d "$INTERNAL_REPDIR" ]; then
    echo "ERROR: Repo directory not found: $INTERNAL_REPDIR"
    exit 1
fi

if [ ! -d "$DEBUGPATH_DIR" ]; then
    echo "ERROR: Debug build directory not found: $DEBUGPATH_DIR"
    exit 1
fi

# ── Step 3: Run upgrade compatibility tests in isolated Docker container ───────

CONTAINER_NAME="upgrade-compat-${PR_NUMBER:-0}_${GITHUB_RUN_NUMBER:-0}_${GITHUB_RUN_ATTEMPT:-0}"

echo ""
echo "=== Step 2/2: Running upgrade compatibility tests in Docker ==="
echo "  Container name: $CONTAINER_NAME"
echo ""

docker run \
    --name "${CONTAINER_NAME}" \
    -v "${CONTAINER_REP_MOUNT}" \
    -v "${CONTAINER_DEBUG_MOUNT}" \
    -v "${GREEN_LOCAL_DIR}:/green_versions" \
    -v "${LOG_DIR}:/upgrade_logs" \
    --rm --ulimit core=-1 \
    tdengine-ci:0.1 \
    ${CONTAINER_SCRIPT}
ret=$?

echo ""
echo "======================================================"
if [ $ret -eq 0 ]; then
    echo "  Upgrade Compatibility Test: PASS"
else
    echo "  Upgrade Compatibility Test: FAIL (exit code: $ret)"
fi
echo "  Log dir: $LOG_DIR"
echo "======================================================"

exit $ret
