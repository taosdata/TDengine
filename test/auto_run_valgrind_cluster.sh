#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TAOSD_BIN="$(find "$SCRIPT_DIR/.." -path '*/build/bin/taosd' -not -path '*/packaging/*' 2>/dev/null | head -n1)"
BUILD_BIN="$(dirname "$TAOSD_BIN")"
BUILD_LIB="$(dirname "$BUILD_BIN")/lib"

if [[ -n "$BUILD_BIN" && -d "$BUILD_BIN" ]]; then
  export PATH="$PATH:$BUILD_BIN"
  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$BUILD_LIB"
fi

python3 "$SCRIPT_DIR/auto_crash_gen_valgrind_cluster.py"
