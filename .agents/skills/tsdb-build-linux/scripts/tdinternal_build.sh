#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_ROOT="/home/simon/dev/TDinternal/debug"

"${SCRIPT_DIR}/tdinternal_exec.sh" "cd ${BUILD_ROOT} && make -j install"
