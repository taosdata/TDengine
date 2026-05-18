#!/usr/bin/env bash
# split-debug.sh — Separate DWARF debug info from binaries/libraries
#
# Usage: ./split-debug.sh <build-dir> [--debug-dir <output-path>]
#
# Processes ELF binaries in <build-dir>/build/bin/ and shared libraries in
# <build-dir>/build/lib/, extracting debug info. GDB auto-discovers debug
# files via the .gnu_debuglink section.
#
# Executables: strip -s (remove all symbols — maximum size reduction)
# Shared libs: strip --strip-debug (keep dynamic symbols required for linking)
#
# Options:
#   --debug-dir <path>  Directory to store .debug files (default: alongside
#                        binaries in <build-dir>/build/{bin,lib}/.debug/)
#
# Examples:
#   ./split-debug.sh debug
#   ./split-debug.sh debug --debug-dir /tmp/debug-symbols
#   ./split-debug.sh /path/to/TDengine/debug --debug-dir ./symbols

set -euo pipefail

usage() {
    echo "Usage: $0 <build-dir> [--debug-dir <output-path>]"
    echo ""
    echo "  <build-dir>           Path to the build output directory (e.g. debug, debug-dev)"
    echo "  --debug-dir <path>    Directory to store .debug files"
    echo "                        Default: <build-dir>/build/{bin,lib}/.debug/"
    exit 1
}

BUILD_DIR=""
DEBUG_OUT_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --debug-dir)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --debug-dir requires an argument"
                exit 1
            fi
            DEBUG_OUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            if [[ -z "$BUILD_DIR" ]]; then
                BUILD_DIR="$1"
            else
                echo "ERROR: unexpected argument: $1"
                usage
            fi
            shift
            ;;
    esac
done

if [[ -z "$BUILD_DIR" ]]; then
    usage
fi

# Resolve to absolute path
if [[ "$BUILD_DIR" != /* ]]; then
    BUILD_DIR="$(pwd)/$BUILD_DIR"
fi

if [[ ! -d "$BUILD_DIR/build" ]]; then
    echo "ERROR: $BUILD_DIR/build does not exist"
    exit 1
fi

# Resolve debug output directory
if [[ -n "$DEBUG_OUT_DIR" ]]; then
    if [[ "$DEBUG_OUT_DIR" != /* ]]; then
        DEBUG_OUT_DIR="$(pwd)/$DEBUG_OUT_DIR"
    fi
    BIN_DEBUG_DIR="$DEBUG_OUT_DIR/bin"
    LIB_DEBUG_DIR="$DEBUG_OUT_DIR/lib"
else
    BIN_DEBUG_DIR="$BUILD_DIR/build/bin/.debug"
    LIB_DEBUG_DIR="$BUILD_DIR/build/lib/.debug"
fi

split_count=0

# --- executables in build/bin/ ---
BIN_DIR="$BUILD_DIR/build/bin"
if [[ -d "$BIN_DIR" ]]; then
    mkdir -p "$BIN_DEBUG_DIR"
    for binary in taosd taos taosql taosmqtt taosudf taosgen taosadapter taoskeeper; do
        path="$BIN_DIR/$binary"
        [[ -f "$path" ]] || continue
        if ! file "$path" | grep -q ELF; then
            echo "[WARN] Skipping non-ELF file: $binary"
            continue
        fi
        echo "[INFO] Splitting debug info: bin/$binary"
        objcopy --only-keep-debug "$path" "$BIN_DEBUG_DIR/$binary.debug"
        strip -s "$path"
        objcopy --add-gnu-debuglink="$BIN_DEBUG_DIR/$binary.debug" "$path"
        split_count=$((split_count + 1))
    done
fi

# --- shared libraries in build/lib/ ---
LIB_DIR="$BUILD_DIR/build/lib"
if [[ -d "$LIB_DIR" ]]; then
    mkdir -p "$LIB_DEBUG_DIR"
    for sofile in libtaos.so libtaosnative.so; do
        path="$LIB_DIR/$sofile"
        [[ -f "$path" ]] || continue
        echo "[INFO] Splitting debug info: lib/$sofile"
        objcopy --only-keep-debug "$path" "$LIB_DEBUG_DIR/$sofile.debug"
        strip --strip-debug "$path"
        objcopy --add-gnu-debuglink="$LIB_DEBUG_DIR/$sofile.debug" "$path"
        split_count=$((split_count + 1))
    done
fi

echo "[INFO] Debug info separated for $split_count files"
echo "[INFO]   bin debug → $BIN_DEBUG_DIR/"
echo "[INFO]   lib debug → $LIB_DEBUG_DIR/"
