#!/bin/bash
# ============================================================================
# setup-macos.sh — TSDB build environment setup for macOS
#
# Usage:
#   ./tools/setup/setup-macos.sh --component engine taosx
#   ./tools/setup/setup-macos.sh --lang rust go
#   ./tools/setup/setup-macos.sh --all
#   ./tools/setup/setup-macos.sh --check --all
#   ./tools/setup/setup-macos.sh --yes --component adapter
#   ./tools/setup/setup-macos.sh --help
# ============================================================================

set -e

SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source framework
source "$SETUP_DIR/utils/common.sh"
source "$SETUP_DIR/utils/platform.sh"
source "$SETUP_DIR/config.sh"

# ── Help ─────────────────────────────────────────────────────────────────────
show_help() {
    cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --component NAME [NAME...]   Setup for specific component(s) (auto-resolves languages)
  --lang NAME [NAME...]        Setup specific language module(s): cpp go rust java node python dotnet
  --all                        Setup all language modules
  --check                      Check-only mode (no modifications)
  --yes, -y                    Auto-confirm all prompts
  --help, -h                   Show this help

Components:
$(list_components)

Examples:
  $(basename "$0") --component engine taosx    # C/C++ + Rust
  $(basename "$0") --lang rust                 # Rust only
  $(basename "$0") --check --all               # Check everything
  $(basename "$0") --yes --all                 # Install everything non-interactively
EOF
    exit 0
}

# ── Parse arguments ──────────────────────────────────────────────────────────
declare -a REQUESTED_LANGS=()
MODE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h)
            show_help
            ;;
        --check|-c)
            CHECK_ONLY=true
            shift
            ;;
        --yes|-y)
            AUTO_YES=true
            shift
            ;;
        --all)
            MODE="all"
            shift
            ;;
        --component)
            MODE="component"
            shift
            while [[ $# -gt 0 && ! "$1" == --* ]]; do
                if [[ "$1" == "all" ]]; then
                    MODE="all"
                    shift
                    break
                fi
                local_langs=$(get_langs_for_component "$1")
                if [[ -z "$local_langs" ]]; then
                    echo "ERROR: Unknown component '$1'"
                    echo "Known components:"
                    list_components
                    exit 1
                fi
                for l in $local_langs; do
                    REQUESTED_LANGS+=("$l")
                done
                shift
            done
            ;;
        --lang)
            MODE="lang"
            shift
            while [[ $# -gt 0 && ! "$1" == --* ]]; do
                REQUESTED_LANGS+=("$1")
                shift
            done
            ;;
        *)
            echo "ERROR: Unknown option '$1'. Use --help for usage."
            exit 1
            ;;
    esac
done

if [[ -z "$MODE" ]]; then
    echo "ERROR: Specify --component, --lang, or --all. Use --help for usage."
    exit 1
fi

# ── Platform check ───────────────────────────────────────────────────────────
init_platform

if [[ "$SETUP_OS" != "macos" ]]; then
    echo "ERROR: This script is for macOS. Use setup-linux.sh on Linux."
    exit 1
fi

# Check Homebrew
if ! cmd_exists brew; then
    echo "ERROR: Homebrew is required on macOS. Install from https://brew.sh"
    exit 1
fi

# ── Resolve language list ────────────────────────────────────────────────────
if [[ "$MODE" == "all" ]]; then
    REQUESTED_LANGS=($ALL_LANG_MODULES)
fi

# Deduplicate
declare -a LANGS=()
for l in "${REQUESTED_LANGS[@]}"; do
    _found=false
    for existing in "${LANGS[@]}"; do
        if [[ "$existing" == "$l" ]]; then _found=true; break; fi
    done
    if [[ "$_found" == "false" ]]; then
        LANGS+=("$l")
    fi
done

if [[ ${#LANGS[@]} -eq 0 ]]; then
    echo "ERROR: No language modules resolved. Use --help for usage."
    exit 1
fi

# ── Init ─────────────────────────────────────────────────────────────────────
detect_shell_rc

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║        TSDB Build Environment Setup (macOS)                    ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  OS:       $SETUP_OS $SETUP_ARCH"
echo "  Pkg mgr:  $PKG_MGR"
echo "  Shell RC: $SHELL_RC"
echo "  Modules:  ${LANGS[*]}"
if [[ "$CHECK_ONLY" == "true" ]]; then
    echo "  Mode:     check-only (no modifications)"
fi
echo ""

# ── Execute modules ──────────────────────────────────────────────────────────
for lang in "${LANGS[@]}"; do
    mod_file="$SETUP_DIR/modules/${lang}.sh"
    if [[ ! -f "$mod_file" ]]; then
        info "Module '${lang}' not yet implemented (skipping)"
        continue
    fi
    source "$mod_file"

    "mod_${lang}_check"

    if [[ "$CHECK_ONLY" != "true" ]]; then
        "mod_${lang}_install"
        "mod_${lang}_config"
    fi
done

# ── Connectivity check (only for loaded modules) ────────────────────────────
header "Internal mirror connectivity"

for lang in "${LANGS[@]}"; do
    [[ ! -f "$SETUP_DIR/modules/${lang}.sh" ]] && continue
    case "$lang" in
        go)   check_url "Go Proxy (Nexus)" "$GO_PROXY" ;;
        rust) check_url "Cargo Registry (Nora)" "https://nora.tdengine.net/cargo/index/config.json" ;;
        cpp)  check_url "Conan Remote (Nexus)" "$CONAN_REMOTE_URL" ;;
    esac
done

# ── Summary ──────────────────────────────────────────────────────────────────
print_summary
