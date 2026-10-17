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

function usage() {
    echo "Usage: $0 -w WORKDIR [-r REPO_DIR] [-l LOG_DIR] [-h]"
    echo ""
    echo "  -w WORKDIR   Working directory (must contain debugNoSan/, e.g. produced by pull-artifacts.sh)"
    echo "  -r REPO_DIR  taos-community source directory (default: \${CI_PROJECT_DIR}/source/taos-community)"
    echo "  -l LOG_DIR   Log output directory (default: WORKDIR/upgrade_compat_logs)"
    echo "  -h           Show help"
}

# ── Configuration ─────────────────────────────────────────────────────────────

# Base URL of the green versions HTTP server
GREEN_HTTP_BASE="http://192.168.1.131/data/nas/TDengine/green_versions"

# Persistent download cache. CI sets this to a worker-local shared directory.
# Versions are prepared before publication and mounted read-only by test jobs.
GREEN_CACHE_DIR=""

# Lock directory for concurrent download safety (co-located with the cache)
LOCK_DIR=""


# ── Argument parsing ──────────────────────────────────────────────────────────

WORKDIR=""
REPO_DIR=""
LOG_DIR=""

while getopts "w:r:l:h" opt; do
    case $opt in
        w) WORKDIR=$OPTARG ;;
        r) REPO_DIR=$OPTARG ;;
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

if [ -z "$REPO_DIR" ]; then
    REPO_DIR="${CI_PROJECT_DIR:+${CI_PROJECT_DIR}/source/taos-community}"
fi

if [ -z "$REPO_DIR" ]; then
    echo "ERROR: -r REPO_DIR is required (or set CI_PROJECT_DIR)"
    usage
    exit 1
fi

if [ ! -d "$REPO_DIR" ]; then
    echo "ERROR: REPO_DIR does not exist: $REPO_DIR"
    exit 1
fi

if [ -z "$LOG_DIR" ]; then
    LOG_DIR="$WORKDIR/upgrade_compat_logs"
fi

GREEN_CACHE_DIR="${GREEN_VERSIONS_CACHE_DIR:-$WORKDIR/green-versions-cache}"
LOCK_DIR="${GREEN_CACHE_DIR}/.locks"

DEBUGPATH_DIR="$WORKDIR/debugNoSan"
# Mount taos-community directly (not via a TDinternal wrapper dir) — REPO_DIR
# may be a path outside WORKDIR, so it must be bind-mounted directly rather
# than through a symlink pointing outside the mount (which would dangle
# inside the container).
CONTAINER_REP_MOUNT="$REPO_DIR:/home/TDinternal/community"
CONTAINER_DEBUG_MOUNT="$DEBUGPATH_DIR:/home/TDinternal/debug"
CONTAINER_SCRIPT="/home/TDinternal/community/test/ci/run_upgrade_compat_container.sh"

mkdir -p "$LOG_DIR"
mkdir -p "$GREEN_CACHE_DIR"
mkdir -p "$LOCK_DIR"

echo "======================================================"
echo "  Upgrade Compatibility Test"
echo "======================================================"
echo "  WORKDIR         : $WORKDIR"
echo "  REPO_DIR        : $REPO_DIR"
echo "  LOG_DIR         : $LOG_DIR"
echo "  GREEN_CACHE_DIR : $GREEN_CACHE_DIR"
echo "======================================================"

# ── Step 1: Download green versions (with cache) ──────────────────────────────

function cache_version_valid() {
    local version_dir="$1"
    [ -f "$version_dir/.complete" ] || return 1
    local file_count valid_count
    file_count=$(find "$version_dir" -maxdepth 1 -type f ! -name .complete 2>/dev/null | wc -l)
    valid_count=$(find "$version_dir" -maxdepth 1 -type f ! -name .complete -size +10M 2>/dev/null | wc -l)
    [ "$file_count" -ge 2 ] && [ "$valid_count" -eq "$file_count" ]
}

function clear_exec_stack_flags() {
    local root_dir="$1"
    python3 - "$root_dir" <<'PYEOF'
import glob
import os
import struct
import sys

PT_GNU_STACK = 0x6474E551
PF_X = 0x1


def clear_exec_stack(path):
    with open(path, "rb") as file:
        data = bytearray(file.read())
    if data[:4] != b"\x7fELF":
        return False
    elf_class = data[4]
    elf_data = data[5]
    endian = "<" if elf_data == 1 else ">"
    if elf_class == 2:
        program_offset, = struct.unpack_from(endian + "Q", data, 0x20)
        entry_size, = struct.unpack_from(endian + "H", data, 0x36)
        entry_count, = struct.unpack_from(endian + "H", data, 0x38)
        flags_offset = 4
    elif elf_class == 1:
        program_offset, = struct.unpack_from(endian + "I", data, 0x1C)
        entry_size, = struct.unpack_from(endian + "H", data, 0x2A)
        entry_count, = struct.unpack_from(endian + "H", data, 0x2C)
        flags_offset = 24
    else:
        return False

    changed = False
    for index in range(entry_count):
        offset = program_offset + index * entry_size
        program_type, = struct.unpack_from(endian + "I", data, offset)
        if program_type != PT_GNU_STACK:
            continue
        flags, = struct.unpack_from(endian + "I", data, offset + flags_offset)
        if flags & PF_X:
            struct.pack_into(endian + "I", data, offset + flags_offset, flags & ~PF_X)
            changed = True

    if changed:
        with open(path, "r+b") as file:
            file.write(data)
    return changed


for library in glob.glob(os.path.join(sys.argv[1], "**", "libtaos*.so*"), recursive=True):
    if not os.path.isfile(library):
        continue
    try:
        if clear_exec_stack(library):
            print(f"[green_versions] cleared exec-stack: {library}")
    except (OSError, struct.error) as error:
        print(f"[green_versions] ERROR: failed to patch {library}: {error}", file=sys.stderr)
        sys.exit(1)
PYEOF
}

function download_version() {
    local version="$1"
    local target_dir="$GREEN_CACHE_DIR/$version"
    local lock_file="$LOCK_DIR/${version}.lock"
    local start_time
    start_time=$(date +%s)

    if cache_version_valid "$target_dir"; then
        return 0
    fi

    # Use flock to prevent concurrent downloads of the same version
    (
        flock -x 200

        # Re-check inside lock in case another process already finished
        if cache_version_valid "$target_dir"; then
            return 0
        fi

        echo "[green_versions] $version: downloading from $GREEN_HTTP_BASE/$version/ ..."
        local staging_dir="${target_dir}.tmp.$$"
        rm -rf "$staging_dir"
        mkdir -p "$staging_dir"

        # Parse HTTP directory listing to get file names
        local file_list
        file_list=$(curl -fsSL "$GREEN_HTTP_BASE/$version/" \
            | sed -n 's/.*href="\([^"]*\)".*/\1/p' \
            | grep -v '/$\|^\.\.$\|^\.$\|^http\|^/' \
            | grep -v '?' \
            | sort -u)

        if [ -z "$file_list" ]; then
            echo "[green_versions] ERROR: $version: failed to list files from $GREEN_HTTP_BASE/$version/"
            rm -rf "$staging_dir"
            return 1
        fi

        echo "[green_versions] $version: files to download: $(echo "$file_list" | tr '\n' ' ')"

        local failed=0
        for fname in $file_list; do
            local url="$GREEN_HTTP_BASE/$version/$fname"
            local dest="$staging_dir/$fname"
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
            rm -rf "$staging_dir"
            return 1
        fi

        if ! clear_exec_stack_flags "$staging_dir"; then
            echo "[green_versions] $version: failed to prepare downloaded libraries"
            rm -rf "$staging_dir"
            return 1
        fi

        touch "$staging_dir/.complete"
        rm -rf "$target_dir"
        mv "$staging_dir" "$target_dir"

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

# Cold upgrade uses four fixed baselines. Hot upgrade uses the oldest available
# version with the same major.minor.patch prefix as the current build.
COLD_VERSIONS="3.3.6.0 3.3.8.0 3.4.0.0 3.4.1.0"
VERSION_FILE="$REPO_DIR/cmake/version.cmake"
CURRENT_VERSION=$(sed -nE \
    's/^[[:space:]]*SET\([[:space:]]*BUILD_VER_NUMBER[[:space:]]+"([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)[^"]*"\).*/\1/p' \
    "$VERSION_FILE" | head -1)
if [ -z "$CURRENT_VERSION" ]; then
    echo "ERROR: Failed to parse BUILD_VER_NUMBER from $VERSION_FILE"
    exit 1
fi
CURRENT_PREFIX=$(echo "$CURRENT_VERSION" | cut -d. -f1-3)
HOT_VERSION=$(printf '%s\n' "$ALL_VERSIONS" \
    | awk -v prefix="${CURRENT_PREFIX}." 'index($0, prefix) == 1' \
    | sort -V | head -1)
REQUIRED_VERSIONS=$(
    {
        printf '%s\n' "$COLD_VERSIONS" | tr ' ' '\n'
        [ -z "$HOT_VERSION" ] || printf '%s\n' "$HOT_VERSION"
    } | awk 'NF && !seen[$0]++'
)
echo "Required green versions: $(echo "$REQUIRED_VERSIONS" | tr '\n' ' ')"

download_failed=0
for ver in $REQUIRED_VERSIONS; do
    download_version "$ver" || {
        echo "ERROR: Failed to download green version $ver"
        download_failed=1
    }
done

if [ $download_failed -ne 0 ]; then
    echo "ERROR: One or more green version downloads failed, aborting."
    exit 1
fi
echo "=== Step 1/2: Required green versions ready ==="

# ── Step 2: Resolve paths ─────────────────────────────────────────────────────

if [ ! -d "$DEBUGPATH_DIR" ]; then
    echo "ERROR: Debug build directory not found: $DEBUGPATH_DIR"
    exit 1
fi

# ── Step 3: Run upgrade compatibility tests in isolated Docker container ───────

CONTAINER_NAME="upgrade-compat-mr${CI_MERGE_REQUEST_IID:-0}_${CI_PIPELINE_ID:-0}_${CI_JOB_ID:-0}"
DOCKER_RUN_PID=""

function cancel_upgrade_compat() {
    local exit_code="$1"
    trap '' TERM INT
    echo "[upgrade-compat] Job cancelled; removing container ${CONTAINER_NAME}"
    timeout 30s docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    if [ -n "${DOCKER_RUN_PID}" ]; then
        kill "${DOCKER_RUN_PID}" 2>/dev/null || true
        wait "${DOCKER_RUN_PID}" 2>/dev/null || true
    fi
    exit "${exit_code}"
}

trap 'cancel_upgrade_compat 143' TERM
trap 'cancel_upgrade_compat 130' INT

echo ""
echo "=== Step 2/2: Running upgrade compatibility tests in Docker ==="
echo "  Container name: $CONTAINER_NAME"
echo ""

docker run \
    --name "${CONTAINER_NAME}" \
    --privileged=true \
    -v "${CONTAINER_REP_MOUNT}" \
    -v "${CONTAINER_DEBUG_MOUNT}" \
    -v "${GREEN_CACHE_DIR}:/green_versions:ro" \
    -v "${LOG_DIR}:/upgrade_logs" \
    --rm --ulimit core=-1 \
    ${DOCKER_IMAGE_NAME:-tdengine-ci:0.3} \
    ${CONTAINER_SCRIPT} &
DOCKER_RUN_PID=$!
wait "${DOCKER_RUN_PID}"
ret=$?
DOCKER_RUN_PID=""

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
