#!/bin/bash
# ============================================================================
# Preheat Rust crate cache via internal registry
#
# Usage: ./preheat-rust.sh [--src PATH]
#
# Runs `cargo fetch` for each primary Rust workspace to populate the internal
# Nora Cargo proxy cache. Designed for scheduled CI jobs.
#
# Prerequisites:
#   - Rust toolchain installed
#   - ~/.cargo/config.toml configured with internal registry
#     (or tools/tsdb-builder/.cargo/config.toml copied in place)
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="${1:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

# Ensure our .cargo/config.toml is used if no user config exists
if [[ ! -f "${HOME}/.cargo/config.toml" ]] && [[ -f "${SCRIPT_DIR}/.cargo/config.toml" ]]; then
    mkdir -p "${HOME}/.cargo"
    cp "${SCRIPT_DIR}/.cargo/config.toml" "${HOME}/.cargo/config.toml"
    echo "[preheat-rust] Installed .cargo/config.toml from tsdb-builder"
fi

echo "[preheat-rust] Source: ${SRC_DIR}"
echo ""

# Primary Rust workspaces with Cargo.lock (these are the ones cmake actually builds)
RUST_WORKSPACES=(
    "source/taos-xservice"
    "source/taos-connector-rust"
    "source/taos-connector-python/taos-ws-py"
)

FAILED=0
TOTAL=0

for ws_rel in "${RUST_WORKSPACES[@]}"; do
    ws_dir="${SRC_DIR}/${ws_rel}"
    if [[ ! -f "${ws_dir}/Cargo.toml" ]]; then
        echo "[SKIP] ${ws_rel} (Cargo.toml not found)"
        continue
    fi
    TOTAL=$((TOTAL + 1))
    echo "[${TOTAL}] Fetching: ${ws_rel}"
    if (cd "${ws_dir}" && cargo fetch 2>&1); then
        echo "  ✓ done"
    else
        echo "  ✗ FAILED"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "[preheat-rust] Complete: ${TOTAL} workspaces processed, ${FAILED} failed"
exit ${FAILED}
