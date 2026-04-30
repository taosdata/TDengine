#!/bin/bash
# Regression guard: verifies that the 3.3.6 server packaging scripts have
# been backported with the non-root (user-mode) support from main.
#
# Checks:
#   install.sh  - function setup_env(), systemctl --user, .install_path, no sudo/csudo
#   remove.sh   - validate_safe_path(), systemctl --user, .install_path, no sudo/csudo
#
# Exit 0 on success, 1 on failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_SH="${PACKAGING_DIR}/tools/install.sh"
REMOVE_SH="${PACKAGING_DIR}/tools/remove.sh"
START_PRE_SH="${PACKAGING_DIR}/tools/startPre.sh"

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

echo "=== community server packaging regression guard ==="
echo

echo "--- install.sh checks ---"
check "function setup_env() exists"           "$INSTALL_SH" "function setup_env()"
check "systemctl --user in install.sh"        "$INSTALL_SH" "systemctl --user"
check ".install_path written in install.sh"   "$INSTALL_SH" ".install_path"
check "non-root install checks for conflicting system-wide install" "$INSTALL_SH" "check_conflicting_system_installation"
check "non-root install explains conflicting system-wide install" "$INSTALL_SH" "A system-wide TDengine installation was detected"
check "log self-link guard in install.sh"     "$INSTALL_SH" '!= "${install_main_dir}/log"'
check "config self-link guard in install.sh"  "$INSTALL_SH" '!= "${install_main_dir}/cfg"'
check "x/explorer units sourced from taosx systemd dir" "$INSTALL_SH" '${script_dir}/${xname}/etc/systemd/system'
check "taosx appended to services when packaged" "$INSTALL_SH" 'services=(${services[@]} ${xname})'
check_function_contains "install_main_path preserves user config" "$INSTALL_SH" "install_main_path" 'if \[\[ \$user_mode -eq 0 \]\]; then'
check "non-root install.sh exports LD_LIBRARY_PATH" "$INSTALL_SH" 'export LD_LIBRARY_PATH=\"${lib_link_dir}:\$LD_LIBRARY_PATH\"'
check "non-root install.sh updates shell rc file" "$INSTALL_SH" 'env_file="${HOME}/.bashrc"'
check "non-root install.sh chooses rc file from login shell" "$INSTALL_SH" 'login_shell="${SHELL##*/}"'
check_function_contains "install_lib cache refresh is root-only" "$INSTALL_SH" "install_lib" 'if \[ "\$user_mode" -eq 0 \]; then'
check_function_contains "install_jemalloc skips user mode" "$INSTALL_SH" "install_jemalloc" 'if \[ "\$user_mode" -ne 0 \]; then'
check_function_contains "install_service_on_systemd skips missing units" "$INSTALL_SH" "install_service_on_systemd" 'if \[ ! -f \${cfg_source_dir}/\$1\.service \]; then'
check "user-mode systemd units export LD_LIBRARY_PATH" "$INSTALL_SH" 'Environment=\"LD_LIBRARY_PATH=${lib_link_dir}\"'
check "user-mode taos.cfg rewrite helper" "$INSTALL_SH" 'set_taos_cfg_value'
check_function_contains "taosx config rewrites data_dir for user mode" "$INSTALL_SH" "install_taosx_config" 'dataDir}/\${xname}'
check_function_contains "taosx config rewrites log path for user mode" "$INSTALL_SH" "install_taosx_config" 'logDir'
check "explorer config prefers packaged taosx source" "$INSTALL_SH" '${script_dir}/${xname}/etc/${PREFIX}/explorer.toml'
check_function_contains "explorer config rewrites data_dir for user mode" "$INSTALL_SH" "install_explorer_config" 'dataDir}/explorer'
check_function_contains "adapter config rewrites taosConfigDir" "$INSTALL_SH" "install_adapter_config" 'taosConfigDir'
check_function_contains "keeper config rewrites log path" "$INSTALL_SH" "install_keeper_config" 'logDir'
check "user-mode taosd unit rewrite" "$INSTALL_SH" 'ExecStart=\"${install_main_dir}/bin/${serverName}\" -c \"${configDir}\"'
check "user-mode taosadapter unit rewrite" "$INSTALL_SH" 'ExecStart=\"${install_main_dir}/bin/${adapterName}\" -c \"${configDir}/${adapterName}.toml\"'
check "user-mode taos-explorer unit rewrite" "$INSTALL_SH" 'ExecStart=\"${install_main_dir}/bin/${explorerName}\" -c \"${configDir}/explorer.toml\"'
check "user-mode taosx unit rewrite" "$INSTALL_SH" 'ExecStart=\"${install_main_dir}/bin/${xname}\" serve -c \"${configDir}/${xname}.toml\"'
check_absent "no sudo/csudo in install.sh"    "$INSTALL_SH" 'sudo|csudo'

echo
echo "--- remove.sh checks ---"
check "validate_safe_path() in remove.sh"     "$REMOVE_SH"  "validate_safe_path"
check "systemctl --user in remove.sh"         "$REMOVE_SH"  "systemctl --user"
check ".install_path used in remove.sh"       "$REMOVE_SH"  ".install_path"
check 'user-mode data_dir default in remove.sh' "$REMOVE_SH" 'data_dir="${installDir}/data"'
check 'user-mode log_dir default in remove.sh'  "$REMOVE_SH" 'log_dir="${installDir}/log"'
check "preserve internal data/cfg/log in remove.sh" "$REMOVE_SH" 'find "${install_main_dir}" -mindepth 1 -maxdepth 1'
check "remove.sh user-mode kill only targets current user" "$REMOVE_SH" 'current_user=$(id -un)'
check_absent "remove.sh avoids broad ps aux grep for service kill" "$REMOVE_SH" 'ps aux \| grep -w \$_service'
check "remove.sh root-mode kill inspects process args" "$REMOVE_SH" 'ps -eo pid=,args='
check "remove.sh root-mode kill scopes by exact -c target" "$REMOVE_SH" 'cfg_arg == config_dir || index(cfg_arg, config_dir "/") == 1'
check "remove.sh root-mode kill handles default root config without -c" "$REMOVE_SH" 'cfg_match == -1 && allow_default_root_config == 1'
check_absent "remove.sh root-mode kill avoids raw substring config matching" "$REMOVE_SH" 'index\(cmd, config_dir\) > 0'
check_absent "remove.sh avoids command-name-only root matching" "$REMOVE_SH" 'ps -eo pid=,comm='
check "remove.sh full cluster includes taosx service" "$REMOVE_SH" 'services=(${PREFIX}"d" ${PREFIX}"adapter" ${PREFIX}"keeper" ${PREFIX}"x" ${PREFIX}"-explorer")'
check "remove.sh appends taosx service when packaged" "$REMOVE_SH" 'services=(${services[@]} ${xName})'
check "remove.sh removes snode data" "$REMOVE_SH" '${data_dir}/snode'
check "remove.sh removes xnode data when present" "$REMOVE_SH" '${data_dir}/xnode'
check "remove.sh removes taosx data" "$REMOVE_SH" '${data_dir}/${PREFIX}x'
check "remove.sh removes explorer data" "$REMOVE_SH" '${data_dir}/explorer'
check "remove.sh removes backup data" "$REMOVE_SH" '${data_dir}/backup'
check "remove.sh cleans bundled taosx uninstall hook" "$REMOVE_SH" '"${install_main_dir}/uninstall_${PREFIX}x.sh"'
check_absent "remove.sh does not execute bundled taosx uninstall hook" "$REMOVE_SH" 'bash \${install_main_dir}/uninstall_\$\{PREFIX\}x\.sh'
check_absent "no sudo/csudo in remove.sh"     "$REMOVE_SH"  'sudo|csudo'

echo
echo "--- startPre.sh checks ---"
check 'startPre.sh uses user service dir' "$START_PRE_SH" '$HOME/.config/systemd/user'
check 'startPre.sh uses systemctl --user' "$START_PRE_SH" 'systemctl --user'

echo
if [[ $fail -gt 0 ]]; then
  echo -e "${RED}FAILED${NC}: $fail check(s) failed, $pass passed"
  exit 1
else
  echo -e "${GREEN}community server packaging guards passed${NC} ($pass/$((pass+fail)))"
  exit 0
fi
