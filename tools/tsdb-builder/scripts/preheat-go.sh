#!/bin/bash
# ============================================================================
# Preheat Go module cache via internal proxy
#
# Usage: ./preheat-go.sh [--src PATH]
#
# Walks all go.mod directories in the source tree and runs `go mod download`
# against the configured GOPROXY. Designed for scheduled CI jobs to keep the
# internal Nexus Go proxy cache warm.
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="${1:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

# Load mirror config from .build-args if available
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    _go_proxy="$(grep -E '^GO_PROXY=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
export GOPROXY="${GOPROXY:-${_go_proxy:-https://nexus.tdengine.net/repository/goproxy/}},direct"
export GONOSUMCHECK="*"
export GONOSUMDB="*"

echo "[preheat-go] GOPROXY=${GOPROXY}"
echo "[preheat-go] Source: ${SRC_DIR}"
echo ""

# Primary Go modules that are actually built by cmake
PRIMARY_MODULES=(
    "source/taos-adapter"
    "source/taos-community/tools/keeper"
    "source/taos-insight"
    "source/taos-connector-go"
    "source/taos-xservice/plugins/opc"
    "source/taos-internal/source/kit/taosDumpTunnel"
    "source/taos-internal/source/plugins/taosainternal"
)

FAILED=0
TOTAL=0

for mod_rel in "${PRIMARY_MODULES[@]}"; do
    mod_dir="${SRC_DIR}/${mod_rel}"
    if [[ ! -f "${mod_dir}/go.mod" ]]; then
        echo "[SKIP] ${mod_rel} (go.mod not found)"
        continue
    fi
    TOTAL=$((TOTAL + 1))
    echo "[${TOTAL}] Downloading: ${mod_rel}"
    if (cd "${mod_dir}" && go mod download 2>&1); then
        echo "  ✓ done"
    else
        echo "  ✗ FAILED"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "[preheat-go] Complete: ${TOTAL} modules processed, ${FAILED} failed"
exit ${FAILED}
