#!/bin/bash
# ============================================================================
# common.sh — shared utility functions for tools/setup
# ============================================================================

# ── Colors (disabled when stdout is not a terminal) ──────────────────────────
if [[ -t 1 ]]; then
    _GREEN='\033[0;32m'
    _YELLOW='\033[0;33m'
    _RED='\033[0;31m'
    _CYAN='\033[0;36m'
    _BOLD='\033[1m'
    _RESET='\033[0m'
else
    _GREEN='' _YELLOW='' _RED='' _CYAN='' _BOLD='' _RESET=''
fi

ok()   { echo -e "  ${_GREEN}✓${_RESET} $*"; }
warn() { echo -e "  ${_YELLOW}⚠${_RESET} $*"; }
fail() { echo -e "  ${_RED}✗${_RESET} $*"; }
info() { echo -e "  ${_CYAN}→${_RESET} $*"; }
header() { echo -e "\n${_BOLD}── $* ──${_RESET}"; }

# ── Globals set by entry script ──────────────────────────────────────────────
AUTO_YES="${AUTO_YES:-false}"
CHECK_ONLY="${CHECK_ONLY:-false}"
SHELL_RC=""
CHANGES_MADE=0
ISSUES_FOUND=0

# ── confirm() — prompt user, respects AUTO_YES and CHECK_ONLY ────────────────
confirm() {
    if [[ "$AUTO_YES" == "true" ]]; then return 0; fi
    if [[ "$CHECK_ONLY" == "true" ]]; then return 1; fi
    local prompt="${1:-Continue?} [Y/n] "
    read -r -p "  $prompt" reply
    [[ -z "$reply" || "$reply" =~ ^[Yy] ]]
}

# ── detect_shell_rc() — find the user's shell RC file ────────────────────────
detect_shell_rc() {
    if [[ -f "$HOME/.zshrc" && "${SHELL:-}" == *zsh* ]]; then
        SHELL_RC="$HOME/.zshrc"
    else
        SHELL_RC="$HOME/.bashrc"
    fi
}

# ── rc_has_line() — check if shell RC contains a string ──────────────────────
rc_has_line() {
    grep -qF "$1" "$SHELL_RC" 2>/dev/null
}

# ── rc_append() — append a line to shell RC with dedup ───────────────────────
# Usage: rc_append "export FOO=bar" "module-name"
rc_append() {
    local line="$1"
    local module="${2:-setup}"
    if rc_has_line "$line"; then
        return 0
    fi
    echo "" >> "$SHELL_RC"
    echo "# TSDB setup ($module)" >> "$SHELL_RC"
    echo "$line" >> "$SHELL_RC"
}

# ── version_gte() — compare semantic versions (a >= b) ──────────────────────
# Usage: version_gte "1.23.4" "1.21" → returns 0 (true)
version_gte() {
    local a="$1" b="$2"
    # printf with %03d pads each segment for string comparison
    local a_norm b_norm
    a_norm=$(echo "$a" | awk -F. '{ printf "%03d%03d%03d", $1, $2, $3 }')
    b_norm=$(echo "$b" | awk -F. '{ printf "%03d%03d%03d", $1, $2, $3 }')
    [[ "$a_norm" > "$b_norm" || "$a_norm" == "$b_norm" ]]
}

# ── cmd_exists() — check if a command is available ───────────────────────────
cmd_exists() {
    command -v "$1" >/dev/null 2>&1
}

# ── check_url() — test HTTP connectivity ─────────────────────────────────────
check_url() {
    local name="$1" url="$2"
    if curl -sf --max-time 10 -o /dev/null "$url" 2>/dev/null; then
        ok "$name ($url)"
        return 0
    else
        fail "$name unreachable ($url)"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
        return 1
    fi
}

# ── backup_file() — create .bak before modifying ────────────────────────────
backup_file() {
    local file="$1"
    if [[ -f "$file" ]]; then
        cp "$file" "${file}.bak"
        info "Backed up ${file} → ${file}.bak"
    fi
}

# ── print_summary() — final status report ───────────────────────────────────
print_summary() {
    header "Summary"
    if [[ "$CHECK_ONLY" == "true" ]]; then
        echo -e "  Check complete. ${ISSUES_FOUND} issue(s) found."
    elif [[ "$CHANGES_MADE" -gt 0 ]]; then
        echo -e "  ${_GREEN}Done${_RESET}: ${CHANGES_MADE} change(s) applied."
        echo ""
        echo -e "  ${_YELLOW}Run to apply:${_RESET} source $SHELL_RC"
    else
        echo -e "  ${_GREEN}Everything is already configured.${_RESET}"
    fi
    if [[ "$ISSUES_FOUND" -gt 0 && "$CHECK_ONLY" != "true" ]]; then
        echo -e "  ${_YELLOW}${ISSUES_FOUND} issue(s) require manual attention (see above).${_RESET}"
    fi
    echo ""
}
