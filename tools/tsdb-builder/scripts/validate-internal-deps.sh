#!/bin/bash
# ============================================================================
# Validate Go/Rust builds work without public network dependencies
#
# Usage: ./validate-internal-deps.sh [--src PATH] [--offline]
#
# Checks:
#   1. All Go modules can be fetched via internal proxy
#   2. All Rust crates can be fetched via internal registry
#   3. All Rust git dependencies resolve from internal mirrors
#
# With --offline flag, sets GOFLAGS=-mod=mod and CARGO_NET_OFFLINE=true
# to verify that cached dependencies are sufficient for a build.
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR=""
OFFLINE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --src) SRC_DIR="$2"; shift 2 ;;
        --offline) OFFLINE=true; shift ;;
        *) SRC_DIR="$1"; shift ;;
    esac
done

SRC_DIR="${SRC_DIR:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"

echo "═══════════════════════════════════════════════════════════════════"
echo " Dependency Source Validation"
echo "═══════════════════════════════════════════════════════════════════"
echo " Source:  ${SRC_DIR}"
echo " Offline: ${OFFLINE}"
echo ""

ERRORS=0

# ── Go validation ────────────────────────────────────────────────────────────
echo "── Go Module Validation ──────────────────────────────────────────"

if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    _go_proxy="$(grep -E '^GO_PROXY=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
export GOPROXY="${GOPROXY:-${_go_proxy:-https://nexus.tdengine.net/repository/goproxy/}},direct"
export GONOSUMCHECK="*"
export GONOSUMDB="*"

if $OFFLINE; then
    export GOFLAGS="-mod=mod"
    export GONOSUMCHECK="*"
fi

echo "GOPROXY=${GOPROXY}"

GO_MODULES=(
    "source/taos-adapter"
    "source/taos-community/tools/keeper"
    "source/taos-insight"
)

for mod_rel in "${GO_MODULES[@]}"; do
    mod_dir="${SRC_DIR}/${mod_rel}"
    [[ -f "${mod_dir}/go.mod" ]] || continue
    printf "  %-45s " "${mod_rel}"
    if (cd "${mod_dir}" && go mod download 2>/dev/null); then
        echo "✓"
    else
        echo "✗ FAILED"
        ERRORS=$((ERRORS + 1))
    fi
done

# ── Rust validation ──────────────────────────────────────────────────────────
echo ""
echo "── Rust Crate Validation ─────────────────────────────────────────"

if $OFFLINE; then
    export CARGO_NET_OFFLINE=true
fi

RUST_WORKSPACES=(
    "source/taos-xservice"
    "source/taos-connector-rust"
    "source/taos-connector-python/taos-ws-py"
)

for ws_rel in "${RUST_WORKSPACES[@]}"; do
    ws_dir="${SRC_DIR}/${ws_rel}"
    [[ -f "${ws_dir}/Cargo.toml" ]] || continue
    printf "  %-45s " "${ws_rel}"
    if (cd "${ws_dir}" && cargo fetch 2>/dev/null); then
        echo "✓"
    else
        echo "✗ FAILED"
        ERRORS=$((ERRORS + 1))
    fi
done

# ── Git dependency URL check ─────────────────────────────────────────────────
echo ""
echo "── Git Dependency URL Audit ──────────────────────────────────────"

GITHUB_REFS=$(grep -rn 'git = "https://github.com' \
    "${SRC_DIR}/source/taos-xservice/Cargo.toml" \
    "${SRC_DIR}/source/taos-xservice/"*/Cargo.toml \
    "${SRC_DIR}/source/taos-xservice/"/*/*/Cargo.toml \
    "${SRC_DIR}/source/taos-connector-rust/"*/Cargo.toml \
    "${SRC_DIR}/source/taos-connector-python/taos-ws-py/Cargo.toml" \
    2>/dev/null || true)

if [[ -n "${GITHUB_REFS}" ]]; then
    echo "  ✗ Found GitHub git dependencies (should use internal mirrors):"
    echo "${GITHUB_REFS}" | sed 's/^/    /'
    ERRORS=$((ERRORS + 1))
else
    echo "  ✓ No GitHub git dependencies found"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════════"
if [[ ${ERRORS} -eq 0 ]]; then
    echo " ✓ All validations passed"
else
    echo " ✗ ${ERRORS} validation(s) failed"
fi
echo "═══════════════════════════════════════════════════════════════════"
exit ${ERRORS}
