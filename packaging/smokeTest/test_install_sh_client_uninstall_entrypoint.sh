#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_SH="${PACKAGING_DIR}/tools/install.sh"

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

pass=0
fail=0

check_client_branch_contains() {
  local description="$1"
  local pattern="$2"

  if awk '/if \[ "\$verType" == "client" \]; then/,/^[[:space:]]*else$/' "$INSTALL_SH" | grep -qF "$pattern"; then
    echo -e "  ${GREEN}PASS${NC}: $description"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: $description"
    echo "       Expected client branch in $INSTALL_SH to contain: '$pattern'"
    ((fail++)) || true
  fi
}

check_client_branch_absent() {
  local description="$1"
  local pattern="$2"

  if awk '/if \[ "\$verType" == "client" \]; then/,/^[[:space:]]*else$/' "$INSTALL_SH" | grep -qF "$pattern"; then
    echo -e "  ${RED}FAIL${NC}: $description"
    echo "       Found forbidden pattern '$pattern' in client branch of $INSTALL_SH"
    ((fail++)) || true
  else
    echo -e "  ${GREEN}PASS${NC}: $description"
    ((pass++)) || true
  fi
}

check_client_uninstall_backend() {
  local client_branch

  client_branch="$(awk '/if \[ "\$verType" == "client" \]; then/,/^[[:space:]]*else$/' "$INSTALL_SH")"

  if grep -qF 'remove_name="remove.sh"' <<<"$client_branch" || grep -qF 'cp -r ${script_dir}/bin/remove.sh ${install_main_dir}/bin' "$INSTALL_SH"; then
    echo -e "  ${GREEN}PASS${NC}: client mode uses remove.sh as uninstall backend"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: client mode uses remove.sh as uninstall backend"
    echo "       Expected install.sh client flow to select or copy remove.sh"
    ((fail++)) || true
  fi
}

echo "=== install.sh client uninstall entrypoint guard ==="
echo

check_client_uninstall_backend
check_client_branch_absent "client mode does not select remove_client.sh" 'remove_name="remove_client.sh"'

echo
if [[ $fail -gt 0 ]]; then
  echo -e "${RED}FAILED${NC}: $fail check(s) failed, $pass passed"
  exit 1
else
  echo -e "${GREEN}install.sh client uninstall entrypoint guard passed${NC} ($pass/$((pass+fail)))"
  exit 0
fi
