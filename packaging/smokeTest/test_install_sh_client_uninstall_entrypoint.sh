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

extract_install_bin_client_branch() {
  awk '
    /^[[:space:]]*function install_bin\(\)/ { in_function = 1 }
    in_function { print }
    in_function && /^[[:space:]]*}/ { exit }
  ' "$INSTALL_SH" | awk '
    /^[[:space:]]*if \[ "\$\{verType\}" == "client" \]; then$/ { in_branch = 1; next }
    in_branch && /^[[:space:]]*else$/ { exit }
    in_branch { print }
  '
}

extract_client_remove_name_branch() {
  awk '
    !in_branch && /^[[:space:]]*if \[ "\$verType" == "client" \]; then$/ { in_branch = 1 }
    in_branch { print }
    in_branch && /^[[:space:]]*else$/ { exit }
  ' "$INSTALL_SH"
}

check_client_branch_absent() {
  local description="$1"
  local pattern="$2"
  local client_branch

  client_branch="$(extract_install_bin_client_branch)"

  if [[ -z "${client_branch}" ]]; then
    echo -e "  ${RED}FAIL${NC}: ${description}"
    echo "       Failed to locate install_bin() client branch in $INSTALL_SH"
    ((fail++)) || true
  elif grep -qF "$pattern" <<<"$client_branch"; then
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
  local remove_name_branch

  client_branch="$(extract_install_bin_client_branch)"
  remove_name_branch="$(extract_client_remove_name_branch)"

  if [[ -z "${client_branch}" ]]; then
    echo -e "  ${RED}FAIL${NC}: client mode uses remove.sh as uninstall backend"
    echo "       Failed to locate install_bin() client branch in $INSTALL_SH"
    ((fail++)) || true
  elif grep -qF 'cp -r ${script_dir}/bin/remove.sh ${install_main_dir}/bin' <<<"$client_branch"; then
    echo -e "  ${GREEN}PASS${NC}: client mode uses remove.sh as uninstall backend"
    ((pass++)) || true
  elif grep -qF 'cp -r ${script_dir}/bin/${remove_name} ${install_main_dir}/bin' <<<"$client_branch" \
    && grep -qF 'remove_name="remove.sh"' <<<"$remove_name_branch" \
    && ! grep -qF 'remove_name="remove_client.sh"' <<<"$remove_name_branch"; then
    echo -e "  ${GREEN}PASS${NC}: client mode uses remove.sh as uninstall backend"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: client mode uses remove.sh as uninstall backend"
    echo "       Expected install.sh client flow to copy remove.sh directly or via remove_name=remove.sh"
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
