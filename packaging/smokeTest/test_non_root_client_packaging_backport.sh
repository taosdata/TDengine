#!/bin/bash
# Regression guard for client packaging non-root support.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_CLIENT_SH="${PACKAGING_DIR}/tools/install_client.sh"
REMOVE_CLIENT_SH="${PACKAGING_DIR}/tools/remove_client.sh"

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

pass=0
fail=0

check() {
  local description="$1"
  local file="$2"
  local pattern="$3"

  if grep -qF "$pattern" "$file"; then
    echo -e "  ${GREEN}PASS${NC}: $description"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: $description"
    echo "       Expected to find: '$pattern' in $file"
    ((fail++)) || true
  fi
}

check_absent() {
  local description="$1"
  local file="$2"
  local pattern="$3"

  if grep -qE "$pattern" "$file"; then
    echo -e "  ${RED}FAIL${NC}: $description"
    echo "       Found forbidden pattern '$pattern' in $file:"
    grep -nE "$pattern" "$file" | head -5 | sed 's/^/         /'
    ((fail++)) || true
  else
    echo -e "  ${GREEN}PASS${NC}: $description"
    ((pass++)) || true
  fi
}

check_function_contains() {
  local description="$1"
  local file="$2"
  local function_name="$3"
  local pattern="$4"

  if awk "/^function ${function_name}\\(\\)/,/^}/" "$file" | grep -qE "$pattern"; then
    echo -e "  ${GREEN}PASS${NC}: $description"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: $description"
    echo "       Expected function ${function_name}() in $file to contain pattern: '$pattern'"
    ((fail++)) || true
  fi
}

echo "=== community client packaging regression guard ==="
echo

echo "--- install_client.sh checks ---"
check "function setup_env() exists" "$INSTALL_CLIENT_SH" "function setup_env()"
check '$HOME/.local/bin in install_client.sh' "$INSTALL_CLIENT_SH" '$HOME/.local/bin'
check ".install_path written in install_client.sh" "$INSTALL_CLIENT_SH" ".install_path"
check "non-root client install checks for conflicting system-wide install" "$INSTALL_CLIENT_SH" "check_conflicting_system_installation"
check "non-root client install explains conflicting system-wide install" "$INSTALL_CLIENT_SH" "A system-wide TDengine installation was detected"
check "log self-link guard in install_client.sh" "$INSTALL_CLIENT_SH" '!= "${install_main_dir}/log"'
check "config self-link guard in install_client.sh" "$INSTALL_CLIENT_SH" '!= "${install_main_dir}/cfg"'
check_function_contains "install_main_path preserves user config" "$INSTALL_CLIENT_SH" "install_main_path" 'if \[ "\$user_mode" -eq 0 \]; then'
check "non-root install_client.sh exports LD_LIBRARY_PATH" "$INSTALL_CLIENT_SH" 'export LD_LIBRARY_PATH=\"${lib_link_dir}:\$LD_LIBRARY_PATH\"'
check "non-root install_client.sh updates shell rc file" "$INSTALL_CLIENT_SH" 'env_file="${HOME}/.bashrc"'
check "non-root install_client.sh chooses rc file from login shell" "$INSTALL_CLIENT_SH" 'login_shell="${SHELL##*/}"'
check_function_contains "install_lib cache refresh is root-only" "$INSTALL_CLIENT_SH" "install_lib" 'if \[ "\$user_mode" -eq 0 \]; then'
check_function_contains "install_jemalloc skips user mode" "$INSTALL_CLIENT_SH" "install_jemalloc" 'if \[ "\$user_mode" -ne 0 \]; then'
check_absent "no sudo/csudo in install_client.sh" "$INSTALL_CLIENT_SH" 'sudo|csudo'

echo
echo "--- remove_client.sh checks ---"
check "validate_safe_path() in remove_client.sh" "$REMOVE_CLIENT_SH" "validate_safe_path"
check '$HOME/.local/bin in remove_client.sh' "$REMOVE_CLIENT_SH" '$HOME/.local/bin'
check ".install_path used in remove_client.sh" "$REMOVE_CLIENT_SH" ".install_path"
check "preserve internal cfg/log in remove_client.sh" "$REMOVE_CLIENT_SH" 'find "${install_main_dir}" -mindepth 1 -maxdepth 1'
check_absent "portable process lookup in remove_client.sh" "$REMOVE_CLIENT_SH" 'ps -C'
check "remove_client.sh uses exact command matching" "$REMOVE_CLIENT_SH" 'ps -eo pid=,user=,comm='
check "remove_client.sh limits user-mode kill to current user" "$REMOVE_CLIENT_SH" 'current_user=$(id -un)'
check "remove_client.sh filters by exact tool names" "$REMOVE_CLIENT_SH" 'client_processes=('
check_absent "remove_client.sh avoids broad grep by clientName2" "$REMOVE_CLIENT_SH" 'grep "\$\{clientName2\}"'
check_absent "no sudo/csudo in remove_client.sh" "$REMOVE_CLIENT_SH" 'sudo|csudo'

echo
if [[ $fail -gt 0 ]]; then
  echo -e "${RED}FAILED${NC}: $fail check(s) failed, $pass passed"
  exit 1
else
  echo -e "${GREEN}community client packaging guards passed${NC} ($pass/$((pass+fail)))"
  exit 0
fi
