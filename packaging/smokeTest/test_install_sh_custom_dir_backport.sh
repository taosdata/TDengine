#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
INSTALL_SH="${REPO_ROOT}/packaging/tools/install.sh"

PASS=0
FAIL=0

check_present() {
  local desc="$1"
  local file="$2"
  local pattern="$3"
  if grep -qE "$pattern" "$file"; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       missing pattern: $pattern"
    FAIL=$((FAIL + 1))
  fi
}

check_function_contains() {
  local desc="$1"
  local file="$2"
  local function_name="$3"
  local pattern="$4"
  if awk "/^function ${function_name}\(\)/,/^}/" "$file" | grep -qE "$pattern"; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       missing in ${function_name}(): $pattern"
    FAIL=$((FAIL + 1))
  fi
}

check_normalize_install_dir_final_path_behavior() {
  local desc="$1"
  local work_dir="${SCRIPT_DIR}/.test_install_sh_custom_dir_backport.$$"
  local helper_file="${work_dir}/normalize_helper.sh"
  local original_home="${HOME}"
  local actual_final=""
  local actual_home_final=""

  rm -rf "$work_dir"
  mkdir -p "$work_dir"

  awk '/^function normalize_install_dir\(\)/,/^}/' "$INSTALL_SH" > "$helper_file"

  # shellcheck source=/dev/null
  source "$helper_file"

  PREFIX="taos"
  HOME="/home/tester"

  actual_final=$(normalize_install_dir "/opt/tdengine/taos")
  actual_home_final=$(normalize_install_dir "~/apps/tdengine/taos")

  HOME="$original_home"

  if [[ "$actual_final" == "/opt/tdengine/taos" && "$actual_home_final" == "/home/tester/apps/tdengine/taos" ]]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       expected final path: /opt/tdengine/taos"
    echo "       actual final path:   ${actual_final:-<empty>}"
    echo "       expected ~ path:     /home/tester/apps/tdengine/taos"
    echo "       actual ~ path:       ${actual_home_final:-<empty>}"
    FAIL=$((FAIL + 1))
  fi

  rm -rf "$work_dir"
}

check_escape_sed_replacement_hash_behavior() {
  local desc="$1"
  local work_dir="${SCRIPT_DIR}/.test_install_sh_custom_dir_backport.$$"
  local helper_file="${work_dir}/escape_helper.sh"
  local raw_value='ExecStartPre="/opt/TDengine#Root/taos/bin/startPre.sh"'
  local expected_value='ExecStartPre="/opt/TDengine\#Root/taos/bin/startPre.sh"'
  local actual_value=""

  rm -rf "$work_dir"
  mkdir -p "$work_dir"

  awk '/^function escape_sed_replacement\(\)/,/^}/' "$INSTALL_SH" > "$helper_file"

  # shellcheck source=/dev/null
  source "$helper_file"

  actual_value=$(escape_sed_replacement "$raw_value")

  if [[ "$actual_value" == "$expected_value" ]]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       expected: $expected_value"
    echo "       actual:   ${actual_value:-<empty>}"
    FAIL=$((FAIL + 1))
  fi

  rm -rf "$work_dir"
}

check_root_taosd_execstartpre_behavior() {
  local desc="$1"
  local work_dir="${SCRIPT_DIR}/.test_install_sh_custom_dir_backport.$$"
  local packaging_dir="${REPO_ROOT}/packaging"
  local helper_file="${work_dir}/rewrite_helpers.sh"
  local service_file="${work_dir}/taosd.service"
  local expected='ExecStartPre="/opt/TDengine Root/taos/bin/startPre.sh"'
  local actual=""

  rm -rf "$work_dir"
  mkdir -p "$work_dir"

  cp "${packaging_dir}/cfg/taosd.service" "$service_file"
  {
    awk '/^function escape_sed_replacement\(\)/,/^}/' "$INSTALL_SH"
    echo
    awk '/^function rewrite_systemd_service_bin_dir\(\)/,/^}/' "$INSTALL_SH"
  } > "$helper_file"

  # shellcheck source=/dev/null
  source "$helper_file"

  PREFIX="taos"
  serverName="taosd"
  bin_dir="/opt/TDengine Root/taos/bin"
  service_config_dir="$work_dir"

  rewrite_systemd_service_bin_dir "$serverName"

  actual=$(grep '^ExecStartPre=' "$service_file" || true)
  if [[ "$actual" == "$expected" ]]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       expected: $expected"
    echo "       actual:   ${actual:-<missing>}"
    FAIL=$((FAIL + 1))
  fi

  rm -rf "$work_dir"
}

check_user_mode_taosd_systemd_rewrite_behavior() {
  local desc="$1"
  local work_dir="${SCRIPT_DIR}/.test_install_sh_custom_dir_backport.$$"
  local packaging_dir="${REPO_ROOT}/packaging"
  local helper_file="${work_dir}/rewrite_helpers.sh"
  local service_file="${work_dir}/taosd.service"
  local expected_exec_start='ExecStart="/home/test/TDengine User/taos/bin/taosd" -c "/home/test/TDengine Config/taos cfg"'
  local expected_exec_start_pre='ExecStartPre="/home/test/TDengine User/taos/bin/startPre.sh"'
  local actual_exec_start=""
  local actual_exec_start_pre=""

  rm -rf "$work_dir"
  mkdir -p "$work_dir"

  cp "${packaging_dir}/cfg/taosd.service" "$service_file"
  {
    awk '/^function escape_sed_replacement\(\)/,/^}/' "$INSTALL_SH"
    echo
    awk '/^function rewrite_systemd_service_for_user_mode\(\)/,/^}/' "$INSTALL_SH"
  } > "$helper_file"

  # shellcheck source=/dev/null
  source "$helper_file"

  sed() {
    if [[ "${1:-}" == "-i" ]]; then
      shift
      local file=""
      local scripts=()

      while [[ "${1:-}" == "-e" ]]; do
        scripts+=("$2")
        shift 2
      done
      file="$1"

      python3 - "$file" "${scripts[@]}" <<'PY'
import re
import sys

file_path = sys.argv[1]
scripts = sys.argv[2:]
with open(file_path, encoding="utf-8") as fh:
    text = fh.read()

def split_sed_fields(body: str, delim: str):
    fields = []
    current = []
    escaped = False
    for ch in body:
        if escaped:
            current.append(ch)
            escaped = False
        elif ch == "\\":
            current.append(ch)
            escaped = True
        elif ch == delim:
            fields.append("".join(current))
            current = []
        else:
            current.append(ch)
    fields.append("".join(current))
    return fields

def unescape_replacement(value: str):
    value = value.replace(r"\#", "#")
    value = value.replace(r"\|", "|")
    value = value.replace(r"\&", "&")
    value = value.replace(r"\\", "\\")
    return value

for script in scripts:
    if script.startswith("s") and len(script) > 1:
        delim = script[1]
        fields = split_sed_fields(script[2:], delim)
        if len(fields) < 2:
            raise SystemExit(f"Unsupported sed substitution: {script}")
        pattern = fields[0]
        replacement = unescape_replacement(fields[1])
        text = re.sub(pattern, lambda _: replacement, text, flags=re.MULTILINE)
    elif script == '/^Environment="LD_LIBRARY_PATH=.*"$/d':
        text = re.sub(r'^Environment="LD_LIBRARY_PATH=.*"$\n?', "", text, flags=re.MULTILINE)
    elif script.startswith('/^\\[Service\\]/a '):
        addition = script[len('/^\\[Service\\]/a '):]
        text = re.sub(r'^\[Service\]$', lambda match: match.group(0) + "\n" + addition, text, count=1, flags=re.MULTILINE)
    else:
        raise SystemExit(f"Unsupported sed command: {script}")

with open(file_path, "w", encoding="utf-8") as fh:
    fh.write(text)
PY
    else
      command sed "$@"
    fi
  }

  user_mode=1
  service_config_dir="$work_dir"
  serverName="taosd"
  adapterName="taosadapter"
  keeperName="taoskeeper"
  explorerName="taos-explorer"
  xname="taosx"
  install_main_dir="/home/test/TDengine User/taos"
  configDir="/home/test/TDengine Config/taos cfg"
  lib_link_dir="/home/test/TDengine User/taos/driver"

  rewrite_systemd_service_for_user_mode "$serverName"

  actual_exec_start=$(grep '^ExecStart=' "$service_file" || true)
  actual_exec_start_pre=$(grep '^ExecStartPre=' "$service_file" || true)
  if [[ "$actual_exec_start" == "$expected_exec_start" && "$actual_exec_start_pre" == "$expected_exec_start_pre" ]]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "       expected ExecStart:    $expected_exec_start"
    echo "       actual ExecStart:      ${actual_exec_start:-<missing>}"
    echo "       expected ExecStartPre: $expected_exec_start_pre"
    echo "       actual ExecStartPre:   ${actual_exec_start_pre:-<missing>}"
    FAIL=$((FAIL + 1))
  fi

  unset -f sed
  rm -rf "$work_dir"
}

echo "=== install.sh custom-dir backport regression guard ==="

bash -n "$INSTALL_SH"
check_present "help advertises -d" "$INSTALL_SH" '^[[:space:]]*-d \[install dir\]'
check_present "help advertises -s" "$INSTALL_SH" '^[[:space:]]*-s[[:space:]]+Silent mode installation'
check_present "help explains -e also skips prompts" "$INSTALL_SH" '^[[:space:]]*-e \[yes \| no\][[:space:]]+Set FQDN interaction; also skip other prompts'
check_present "getopts accepts -d and -s" "$INSTALL_SH" 'getopts "[^"]*d:[^"]*s'
check_present "taos_dir_set state exists" "$INSTALL_SH" '^taos_dir_set=0$'
check_present "silent_mode state exists" "$INSTALL_SH" '^silent_mode=0$'
check_present "upgrade reuses .install_path" "$INSTALL_SH" '\.install_path'
check_present "-d uses normalize helper" "$INSTALL_SH" 'taos_dir=\$\(normalize_install_dir "\$OPTARG"\)'
check_present "interactive custom-dir prompt exists" "$INSTALL_SH" 'Please input new install directory'
check_present "interactive custom-dir uses normalize helper" "$INSTALL_SH" 'taos_dir=\$\(normalize_install_dir "\$new_dir"\)'
check_normalize_install_dir_final_path_behavior "normalize helper keeps explicit final taos dirs unchanged"
check_function_contains "root keeps data in /var/lib" "$INSTALL_SH" 'setup_env' 'dataDir="/var/lib/\$\{PREFIX\}"'
check_function_contains "root keeps config in /etc" "$INSTALL_SH" 'setup_env' 'configDir="/etc/\$\{PREFIX\}"'
check_function_contains "non-root keeps data under installDir" "$INSTALL_SH" 'setup_env' 'dataDir="\$\{installDir\}/data"'
check_function_contains "setup_env probes /usr/bin taosd link when PATH is stale" "$INSTALL_SH" 'setup_env' '"/usr/bin/\$\{serverName\}"'
check_function_contains "setup_env probes /usr/bin taos client link when PATH is stale" "$INSTALL_SH" 'setup_env' '"/usr/bin/\$\{clientName\}"'
check_function_contains "setup_env probes ~/.local/bin taosd link when PATH is stale" "$INSTALL_SH" 'setup_env' '"\$HOME/\.local/bin/\$\{serverName\}"'
check_function_contains "setup_env probes ~/.local/bin taos client link when PATH is stale" "$INSTALL_SH" 'setup_env' '"\$HOME/\.local/bin/\$\{clientName\}"'
check_function_contains "setup_env resolves probed install dir helper" "$INSTALL_SH" 'setup_env' 'probe_install_dir=\$\(resolve_install_dir_from_binary "\$probe_bin" \|\| true\)'
check_function_contains "install_main_path quotes custom install dirs" "$INSTALL_SH" 'install_main_path' 'rm -rf "\$\{install_main_dir\}/cfg"'
check_function_contains "install_bin quotes custom install dir copies" "$INSTALL_SH" 'install_bin' 'cp -r "\$\{script_dir\}/bin/\$\{clientName\}" "\$\{install_main_dir\}/bin"'
check_function_contains "install_taosd_config quotes config symlink path" "$INSTALL_SH" 'install_taosd_config' 'ln -sf "\$\{configDir\}/\$\{configFile\}" "\$\{install_main_dir\}/cfg"'
check_function_contains "install_log quotes symlink target and path" "$INSTALL_SH" 'install_log' 'ln -sf "\$\{logDir\}" "\$\{install_main_dir\}/log"'
check_function_contains "install_data quotes symlink target and path" "$INSTALL_SH" 'install_data' 'ln -sf "\$\{dataDir\}" "\$\{install_main_dir\}/data"'
check_function_contains "systemd install rewrites template bin dir for custom root installs" "$INSTALL_SH" 'install_service_on_systemd' 'rewrite_systemd_service_bin_dir "\$1"'
check_function_contains "systemd bin-dir rewrite stays root-only to preserve user-mode quoting" "$INSTALL_SH" 'install_service_on_systemd' 'if \[ "\$user_mode" -eq 0 \]; then'
check_function_contains "systemd bin-dir rewrite replaces /usr/local template path" "$INSTALL_SH" 'rewrite_systemd_service_bin_dir' 's#/usr/local/\$\{PREFIX\}/bin#'
check_function_contains "root taosd systemd rewrite quotes full ExecStartPre path" "$INSTALL_SH" 'rewrite_systemd_service_bin_dir' 'exec_start_pre="ExecStartPre=\\"\$\{bin_dir\}/startPre\.sh\\""'
check_function_contains "root taosd systemd rewrite replaces full ExecStartPre line" "$INSTALL_SH" 'rewrite_systemd_service_bin_dir' 's#\^ExecStartPre=/usr/local/taos/bin/startPre\.sh\.\*#\$\{escaped_exec_start_pre\}#'
check_root_taosd_execstartpre_behavior "root taosd systemd rewrite keeps ExecStartPre quoted after bin-dir rewrite"
check_function_contains "user-mode systemd rewrite quotes taosd binary path" "$INSTALL_SH" 'rewrite_systemd_service_for_user_mode' 'ExecStart=\\"\$\{install_main_dir\}/bin/\$\{serverName\}\\" -c \\"\$\{configDir\}\\"'
check_function_contains "user-mode systemd rewrite escapes sed replacements" "$INSTALL_SH" 'rewrite_systemd_service_for_user_mode' 'escape_sed_replacement'
check_user_mode_taosd_systemd_rewrite_behavior "user-mode taosd systemd rewrite keeps ExecStart and ExecStartPre quoted for custom paths"
check_escape_sed_replacement_hash_behavior "escape_sed_replacement escapes # for # delimited rewrites"
check_function_contains "set_taos_cfg_value escapes replacement values" "$INSTALL_SH" 'set_taos_cfg_value' 'escaped_value=\$\(escape_sed_replacement "\$value"\)'
check_function_contains "config rewrites escape quoted data dir values" "$INSTALL_SH" 'install_taosx_config' 'escaped_data_dir=\$\(escape_sed_replacement'
check_function_contains "version probe quotes existing server path" "$INSTALL_SH" 'is_version_compatible' 'exist_version=\$\("\$\{installDir\}/bin/\$\{serverName\}" -V'
check_function_contains "setup_env quotes detected taosd path for version probe" "$INSTALL_SH" 'setup_env' 'taosd_ver=\$\("\$\{real_taosd_bin\}" -V'

if [ "$FAIL" -eq 0 ]; then
  echo "PASS: $PASS checks"
else
  echo "FAIL: $FAIL checks failed, $PASS passed"
  exit 1
fi
