#!/usr/bin/env bash
# --------------------------------------------------------------------------
# monorepo-cargo-patch.sh — Redirect git dependencies to local monorepo paths
# --------------------------------------------------------------------------
# When building taos-xservice inside the tsdb monorepo, Cargo.toml declares
# taos-connector-rust as a GitHub git dependency. On internal networks where
# github.com is unreachable, this causes "rev not found" errors.
#
# This script appends a [patch] section that redirects the git dependency
# to the local monorepo checkout (source/taos-connector-rust), eliminating
# network access for that dependency.
#
# Usage:
#   ./tools/setup/monorepo-cargo-patch.sh           # apply patch
#   ./tools/setup/monorepo-cargo-patch.sh --revert   # remove patch
#
# The script is idempotent — running it multiple times is safe.
# --------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CARGO_TOML="$REPO_ROOT/source/taos-xservice/Cargo.toml"
CONNECTOR_PATH="$REPO_ROOT/source/taos-connector-rust/taos"

PATCH_MARKER="# >>> monorepo-cargo-patch (auto-generated, do not edit) <<<"
PATCH_END="# <<< monorepo-cargo-patch end <<<"

usage() {
    echo "Usage: $0 [--revert]"
    echo "  (no args)  Apply monorepo path patches to taos-xservice/Cargo.toml"
    echo "  --revert   Remove the monorepo path patches"
    exit 1
}

revert_patch() {
    if ! grep -q "$PATCH_MARKER" "$CARGO_TOML" 2>/dev/null; then
        echo "[INFO] No monorepo patch found in $CARGO_TOML — nothing to revert."
        return 0
    fi
    # Remove everything between PATCH_MARKER and PATCH_END (inclusive)
    # Use perl for portable in-place editing (macOS sed -i requires different syntax)
    perl -i -ne "print unless /\Q$PATCH_MARKER\E/ .. /\Q$PATCH_END\E/" "$CARGO_TOML"
    echo "[OK] Reverted monorepo cargo patch from $CARGO_TOML"
}

apply_patch() {
    if [ ! -f "$CARGO_TOML" ]; then
        echo "[ERROR] $CARGO_TOML not found. Are you in the tsdb monorepo?" >&2
        exit 1
    fi
    if [ ! -d "$CONNECTOR_PATH" ]; then
        echo "[ERROR] $CONNECTOR_PATH not found. Ensure source/taos-connector-rust is checked out." >&2
        exit 1
    fi

    # Idempotent: revert first if already applied
    if grep -q "$PATCH_MARKER" "$CARGO_TOML" 2>/dev/null; then
        echo "[INFO] Patch already present — refreshing."
        revert_patch
    fi

    cat >> "$CARGO_TOML" <<EOF

$PATCH_MARKER
[patch."https://github.com/taosdata/taos-connector-rust.git"]
taos = { path = "../taos-connector-rust/taos" }
$PATCH_END
EOF

    echo "[OK] Applied monorepo cargo patch to $CARGO_TOML"
    echo "     taos → ../taos-connector-rust/taos (local path)"
}

case "${1:-}" in
    --revert) revert_patch ;;
    --help|-h) usage ;;
    "") apply_patch ;;
    *) echo "[ERROR] Unknown option: $1" >&2; usage ;;
esac
