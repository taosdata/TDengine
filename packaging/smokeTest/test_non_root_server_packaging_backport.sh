#!/bin/bash
# Regression guard: verifies that the 3.3.6 server packaging scripts have
# been backported with the non-root (user-mode) support from main.
#
# Checks:
#   install.sh  - function setup_env(), systemctl --user, .install_path
#   remove.sh   - validate_safe_path(), systemctl --user, .install_path
#
# Exit 0 on success, 1 on failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_SH="${PACKAGING_DIR}/tools/install.sh"
REMOVE_SH="${PACKAGING_DIR}/tools/remove.sh"

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

echo "=== community server packaging regression guard ==="
echo

echo "--- install.sh checks ---"
check "function setup_env() exists"           "$INSTALL_SH" "function setup_env()"
check "systemctl --user in install.sh"        "$INSTALL_SH" "systemctl --user"
check ".install_path written in install.sh"   "$INSTALL_SH" ".install_path"

echo
echo "--- remove.sh checks ---"
check "validate_safe_path() in remove.sh"     "$REMOVE_SH"  "validate_safe_path"
check "systemctl --user in remove.sh"         "$REMOVE_SH"  "systemctl --user"
check ".install_path used in remove.sh"       "$REMOVE_SH"  ".install_path"

echo
if [[ $fail -gt 0 ]]; then
  echo -e "${RED}FAILED${NC}: $fail check(s) failed, $pass passed"
  exit 1
else
  echo -e "${GREEN}community server packaging guards passed${NC} ($pass/$((pass+fail)))"
  exit 0
fi
