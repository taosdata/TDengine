#!/bin/bash
# shellcheck disable=SC1091
# This file is used to install analysis platform on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e

serverFqdn=""

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
echo -e "${script_dir}"

custom_dir_set=0
all_venv=0
offline_mode=0
python_home=""
venv_package=""
model_venv_package=""
model_package=""
install_mode="system-python"

show_help() {
  echo "Usage: $(basename "$0") -d [install dir] -a -o [offline package options]"
  echo
  echo "Help:"
  echo "  -d [install dir]   Specify installation directory"
  echo "  -a                 Install all model virtual environments"
  echo "  -o                 Offline mode: skip reinstalling existing virtual environments"
  echo "  --python-home <path>"
  echo "                     Existing Python runtime directory prepared by Apex"
  echo "  --venv-package <path>"
  echo "                     TDgpt main virtual environment package"
  echo "  --model-venv-package <path>"
  echo "                     TDgpt model virtual environments package"
  echo "  --model-package <path>"
  echo "                     TDgpt model resource package"
  echo
  echo "Environment variables for pip mirror (optional):"
  echo "  PIP_INDEX_URL  Set pip index mirror, e.g. https://pypi.tuna.tsinghua.edu.cn/simple"
  echo
  echo "Example:"
  echo "  PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple bash $0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d)
      customDir="${2:-}"
      [ -n "$customDir" ] || { echo "Missing value for -d"; exit 1; }
      custom_dir_set=1
      shift 2
      ;;
    -a)
      all_venv=1
      shift
      ;;
    -o)
      offline_mode=1
      shift
      ;;
    --python-home)
      python_home="${2:-}"
      [ -n "$python_home" ] || { echo "Missing value for --python-home"; exit 1; }
      shift 2
      ;;
    --python-home=*)
      python_home="${1#*=}"
      shift
      ;;
    --venv-package|--offline-package)
      venv_package="${2:-}"
      [ -n "$venv_package" ] || { echo "Missing value for $1"; exit 1; }
      offline_mode=1
      shift 2
      ;;
    --venv-package=*|--offline-package=*)
      venv_package="${1#*=}"
      offline_mode=1
      shift
      ;;
    --model-venv-package)
      model_venv_package="${2:-}"
      [ -n "$model_venv_package" ] || { echo "Missing value for --model-venv-package"; exit 1; }
      offline_mode=1
      shift 2
      ;;
    --model-venv-package=*)
      model_venv_package="${1#*=}"
      offline_mode=1
      shift
      ;;
    --model-package)
      model_package="${2:-}"
      [ -n "$model_package" ] || { echo "Missing value for --model-package"; exit 1; }
      offline_mode=1
      shift 2
      ;;
    --model-package=*)
      model_package="${1#*=}"
      offline_mode=1
      shift
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Usage: $0 [-d install_dir] [-a] [-o]"
      exit 1
      ;;
  esac
done

# Dynamic directory
PREFIX="taos"
PRODUCTPREFIX="taosanode"
serverName="${PRODUCTPREFIX}d"
configFile="taosanode.config.py"
productName="TDengine TSDB TDgpt"
emailName="taosdata.com"
tarName="package.tar.gz"
global_conf_dir="/etc/${PREFIX}"
tar_td_model_name="tdtsfm.tar.gz"
tar_xhs_model_name="timemoe.tar.gz"

python_minor_ver=0  #check the python version
bin_link_dir="/usr/bin"
# if install python venv
install_venv="${INSTALL_VENV:-True}"
lite_mode=0
[ -f "${script_dir}/.tdgpt-lite" ] && lite_mode=1

# default env:transformers 4.40.0
if [ $custom_dir_set -eq 1 ]; then
  installDir="${customDir}/${PREFIX}/${PRODUCTPREFIX}"
  logDir="${installDir}/log"
  dataDir="${installDir}/data"
  modelDir="${dataDir}/model"
  imgDir="${dataDir}/img"
  resourceDir="${dataDir}/resource"
  venvDir="${dataDir}/venv"
else
  installDir="/usr/local/${PREFIX}/${PRODUCTPREFIX}"
  logDir="/var/log/${PREFIX}/${PRODUCTPREFIX}"
  dataDir="/var/lib/${PREFIX}/${PRODUCTPREFIX}"
  modelDir="${dataDir}/model"
  imgDir="${dataDir}/img"
  resourceDir="${dataDir}/resource"
  venvDir="${dataDir}/venv"
fi
modelVenvsDir="${dataDir}/model_venvs"

# get pip mirror and index url from env
PIP_INDEX_URL="${PIP_INDEX_URL:-}"

# build pip extra args
pip_extra_args=()
if [ -n "$PIP_INDEX_URL" ]; then
  pip_extra_args+=(-i "$PIP_INDEX_URL")
fi

function resolve_requirement_file() {
  local req_name="$1"
  if [ -f "${script_dir}/${req_name}" ]; then
    echo "${script_dir}/${req_name}"
  elif [ -f "${script_dir}/../${req_name}" ]; then
    echo "${script_dir}/../${req_name}"
  else
    echo -e "${RED}Error: Requirements file not found: ${req_name}${NC}"
    echo -e "${RED}Searched in:${NC}"
    echo -e "${RED}  - ${script_dir}/${req_name}${NC}"
    echo -e "${RED}  - ${script_dir}/../${req_name}${NC}"
    return 1
  fi
}

function run_venv_pip() {
  local -a pip_cmd=("${venvDir}/bin/python" -m pip)
  if [ -n "${csudo}" ]; then
    pip_cmd=(sudo "${pip_cmd[@]}")
  fi
  "${pip_cmd[@]}" "$@"
}

#install main path
install_main_dir=${installDir}
install_state_file="${install_main_dir}/cfg/install-state.json"
install_state_dir="${install_main_dir}/cfg"
python_runtime_dir="${python_home:-${PYTHON_RUNTIME_DIR:-}}"
service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

update_flag=0
prompt_force=0

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &>/dev/null; then
  service_mod=0
elif $(which service &>/dev/null); then
  service_mod=1
  service_config_dir="/etc/init.d"
  if $(which chkconfig &>/dev/null); then
    initd_mod=1
  elif $(which insserv &>/dev/null); then
    initd_mod=2
  elif $(which update-rc.d &>/dev/null); then
    initd_mod=3
  else
    service_mod=2
  fi
else
  service_mod=2
fi

# get the operating system type for using the corresponding init file
# ubuntu/debian(deb), centos/fedora(rpm), others: opensuse, redhat, ..., no verification
#osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if [[ -e /etc/os-release ]]; then
  osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2) || :
else
  osinfo=""
fi
#echo "osinfo: ${osinfo}"
os_type=0
if echo $osinfo | grep -qwi "ubuntu"; then
  #  echo "This is ubuntu system"
  os_type=1
elif echo $osinfo | grep -qwi "debian"; then
  #  echo "This is debian system"
  os_type=1
elif echo $osinfo | grep -qwi "Kylin"; then
  #  echo "This is Kylin system"
  os_type=1
elif echo $osinfo | grep -qwi "centos"; then
  #  echo "This is centos system"
  os_type=2
elif echo $osinfo | grep -qwi "fedora"; then
  #  echo "This is fedora system"
  os_type=2
elif echo $osinfo | grep -qwi "Linux"; then
  #  echo "This is Linux system"
  os_type=1
  service_mod=0
  initd_mod=0
  service_config_dir="/etc/systemd/system"
else
  echo " osinfo: ${osinfo}"
  echo " This is an officially unverified linux system,"
  echo " if there are any problems with the installation and operation, "
  echo " please feel free to contact ${emailName} for support."
  os_type=1
fi

# =============================  get input parameters =================================================
# set parameters by default value
verType=server      # [server | client]
initType=systemd    # [systemd | service | ...]

services=(${serverName})

function install_services() {
  for service in "${services[@]}"; do
    install_service ${service}
  done
}

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 "$pid" || :
  fi
}

function kill_model_service() {
  for script in stop-tdtsfm.sh stop-time-moe.sh; do
    script_path="${installDir}/bin/${script}"
    [ -f "${script_path}" ] && ${csudo}bash "${script_path}" || :
  done
}

function install_main_path() {
  # only delete non-data/log/cfg files
  if [ -d "${install_main_dir}" ]; then
    find "${install_main_dir}" -mindepth 1 -maxdepth 1 \
      ! -name 'data' ! -name 'log' ! -name 'cfg' \
      -exec ${csudo}rm -rf {} +
  fi

  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}mkdir -p ${install_main_dir}/cfg
  ${csudo}mkdir -p ${install_main_dir}/bin
  ${csudo}mkdir -p ${install_main_dir}/lib
  ${csudo}mkdir -p ${global_conf_dir}
}

function install_bin_and_lib() {
  ${csudo}cp -r ${script_dir}/bin/* ${install_main_dir}/bin
  ${csudo}cp -r ${script_dir}/lib/* ${install_main_dir}/lib/

  # Handle rmtaosanode separately
  sed -i.bak \
  -e "s|/usr/local/taos/taosanode|${install_main_dir}|g" \
  -e "s|/var/lib/taos/taosanode|${dataDir}|g" \
  -e "s|/var/log/taos/taosanode|${logDir}|g" \
  "${install_main_dir}/bin/uninstall.sh"
  rm -f "${install_main_dir}/bin/uninstall.sh.bak"
  [ -L "${bin_link_dir}/rmtaosanode" ] && ${csudo}rm -rf "${bin_link_dir}/rmtaosanode" || :
  ${csudo}ln -s "${install_main_dir}/bin/uninstall.sh" "${bin_link_dir}/rmtaosanode"

  # Create an array of link names and target scripts
  declare -A links=(
    ["start-tdtsfm"]="${install_main_dir}/bin/start-tdtsfm.sh"
    ["stop-tdtsfm"]="${install_main_dir}/bin/stop-tdtsfm.sh"
    ["start-time-moe"]="${install_main_dir}/bin/start-time-moe.sh"
    ["stop-time-moe"]="${install_main_dir}/bin/stop-time-moe.sh"
    ["start-model"]="${install_main_dir}/bin/start-model.sh"
    ["stop-model"]="${install_main_dir}/bin/stop-model.sh"
    ["start-model-from-remote"]="${install_main_dir}/bin/start_model_from_remote.sh"
  )

  # Iterate over the array and create/remove links as needed
  for link in "${!links[@]}"; do
    target="${links[$link]}"
    [ -L "${bin_link_dir}/${link}" ] && ${csudo}rm -rf "${bin_link_dir}/${link}" || :
    ${csudo}ln -s "${target}" "${bin_link_dir}/${link}"
  done

  ${csudo}rm -f "${bin_link_dir}/taosanodectl"
  launcher_file="$(mktemp)"
  cat > "${launcher_file}" <<EOF
#!/bin/sh
exec "${dataDir}/venv/bin/python3" "${install_main_dir}/bin/taosanodectl.py" "\$@"
EOF
  ${csudo}install -m 0755 "${launcher_file}" "${bin_link_dir}/taosanodectl"
  rm -f "${launcher_file}"
}

function install_anode_config() {
  fileName="${script_dir}/cfg/${configFile}"
  echo -e $fileName

  if [ -f ${fileName} ]; then
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$serverFqdn/" "${fileName}"

    sed -i.bak \
      -e "s|/usr/local/taos/taosanode|${install_main_dir}|g" \
      -e "s|/var/lib/taos/taosanode|${dataDir}|g" \
      -e "s|/var/log/taos/taosanode|${logDir}|g" \
      "${fileName}"

    if [ -f "${global_conf_dir}/${configFile}" ]; then
      ${csudo}cp ${fileName} ${global_conf_dir}/${configFile}.new
    else
      ${csudo}cp ${fileName} ${global_conf_dir}/${configFile}
    fi
  fi

  ${csudo}ln -sf ${global_conf_dir}/${configFile} "${install_main_dir}/cfg"
}

function install_config() {

  [ ! -z $1 ] && return 0 || : # only install client

  install_anode_config

  echo
  echo -e -n "${GREEN}Enter FQDN:port (like h1.${emailName}:6030) of an existing ${productName} cluster node to join${NC}"
  echo
  echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
  read firstEp
  while true; do
    if [ ! -z "$firstEp" ]; then
      if [ -f ${global_conf_dir}/${configFile} ]; then
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${global_conf_dir}/${configFile}
      else
        ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${script_dir}/cfg/${configFile}
      fi
      break
    else
      break
    fi
  done

  echo
  echo -e -n "${GREEN}Enter your email address for priority support or enter empty to skip${NC}: "
  read emailAddr
  while true; do
    if [ ! -z "$emailAddr" ]; then
      email_file="${install_main_dir}/email"
      ${csudo}bash -c "echo $emailAddr > ${email_file}"
      break
    else
      break
    fi
  done
}

function install_log() {
  ${csudo}mkdir -p ${logDir} && ${csudo}chmod 777 ${logDir}
  if [ ${custom_dir_set} -eq 0 ];then 
    ${csudo}ln -sf ${logDir} "${install_main_dir}/log"
  fi
}

function install_model_dirs() {
  ${csudo}mkdir -p "${modelDir}" "${modelVenvsDir}"
  ${csudo}chmod 777 "${modelDir}" "${modelVenvsDir}"
  if [ ${custom_dir_set} -eq 0 ];then 
    ${csudo}ln -sf ${modelDir} "${install_main_dir}/model"
  fi
}

function install_model() {
  if [ -n "$model_package" ]; then
    echo -e "Import TDgpt model resource package: ${model_package}"
    extract_offline_package_to_dir "$model_package" "$modelDir" "TDgpt model resource package"
    echo -e "TDgpt model resource package imported to ${modelDir}"
    return 0
  fi

  # Default models: extract them into modelDir
  [ -f "${script_dir}/model/${tar_td_model_name}" ] && tar -zxf "${script_dir}/model/${tar_td_model_name}" -C "${modelDir}" || :
  [ -f "${script_dir}/model/${tar_xhs_model_name}" ] && tar -zxf "${script_dir}/model/${tar_xhs_model_name}" -C "${modelDir}" || :

   # In all_venv mode, directly extract specified model packages into modelDir
  if [ ${all_venv} -eq 1 ]; then
    for extra_model in chronos moment-large moirai timesfm; do
      model_tar="${script_dir}/model/${extra_model}.tar.gz"
      [ -f "$model_tar" ] && tar -zxf "$model_tar" -C "${modelDir}" || :
    done
  fi
}

function install_img() {
  ${csudo}mkdir -p ${imgDir} && ${csudo}chmod 777 ${imgDir}
  if [ ${custom_dir_set} -eq 0 ];then
    ${csudo}ln -sf ${imgDir} "${install_main_dir}/img"
  fi
}

function install_resource() {
  ${csudo}mkdir -p ${resourceDir} && ${csudo}chmod 777 ${resourceDir}
  if [ ${custom_dir_set} -eq 0 ];then 
    ${csudo}ln -sf ${resourceDir} ${install_main_dir}/resource
  fi
  ${csudo}cp ${script_dir}/resource/*.sql ${resourceDir}/
}

function require_existing_path() {
  local path="$1"
  local desc="$2"
  if [ -z "$path" ]; then
    echo -e "\033[31m${desc} path is empty.\033[0m"
    exit 1
  fi
  if [ ! -e "$path" ]; then
    echo -e "\033[31m${desc} not found: ${path}\033[0m"
    exit 1
  fi
}

function validate_offline_packages() {
  [ -n "$python_home" ] && require_existing_path "$python_home" "Python home"
  [ -n "$venv_package" ] && require_existing_path "$venv_package" "TDgpt venv package"
  [ -n "$model_venv_package" ] && require_existing_path "$model_venv_package" "TDgpt model venv package"
  [ -n "$model_package" ] && require_existing_path "$model_package" "TDgpt model resource package"
  return 0
}

function has_offline_resource_args() {
  [ -n "$python_home" ] || [ -n "$venv_package" ] || [ -n "$model_venv_package" ] || [ -n "$model_package" ]
}

function detect_requested_install_mode() {
  if has_offline_resource_args; then
    install_mode="offline-resource"
  else
    install_mode="system-python"
  fi
}

function read_existing_install_mode() {
  if [ ! -f "$install_state_file" ]; then
    return 1
  fi
  awk -F'"' '/"install_mode"[[:space:]]*:/ {print $4; exit}' "$install_state_file"
}

function guard_install_mode_transition() {
  local existing_mode
  existing_mode="$(read_existing_install_mode || true)"
  if [ "$existing_mode" = "offline-resource" ] && [ "$install_mode" = "system-python" ]; then
    echo -e "\033[31mCurrent TDgpt installation uses offline resource mode.\033[0m"
    echo -e "\033[31mPlease rerun install.sh with offline resource parameters instead of falling back to system Python mode.\033[0m"
    exit 1
  fi
}

function write_install_state() {
  ${csudo}mkdir -p "$install_state_dir"
  local tmp_file
  tmp_file=$(mktemp)
  cat > "$tmp_file" <<EOF
{
  "install_mode": "${install_mode}",
  "python_home": "${python_home}",
  "venv_package": "${venv_package}",
  "model_venv_package": "${model_venv_package}",
  "model_package": "${model_package}"
}
EOF
  ${csudo}cp "$tmp_file" "$install_state_file"
  ${csudo}chmod 644 "$install_state_file"
  rm -f "$tmp_file"
}

function create_target_stage_dir() {
  local target_dir="$1"
  local target_parent
  local target_name

  target_parent=$(dirname "$target_dir")
  target_name=$(basename "$target_dir")
  ${csudo}mkdir -p "$target_parent"
  ${csudo}mktemp -d "${target_parent}/.${target_name}.apex-extract.XXXXXX"
}

function cleanup_target_stage_dirs() {
  local target_dir="$1"
  local target_parent
  local target_name

  target_parent=$(dirname "$target_dir")
  target_name=$(basename "$target_dir")
  ${csudo}mkdir -p "$target_parent"
  ${csudo}find "$target_parent" -mindepth 1 -maxdepth 1 -type d \
    -name ".${target_name}.apex-extract.*" -exec rm -rf {} +
}

function package_fingerprint() {
  local package_path="$1"
  local package_size
  local package_mtime

  package_size=$(stat -c '%s' "$package_path")
  package_mtime=$(stat -c '%Y' "$package_path")
  printf '%s|%s|%s\n' "$(basename "$package_path")" "$package_size" "$package_mtime"
}

function has_matching_venv_package_marker() {
  local package_path="$1"
  local target_dir="$2"
  local marker_file="${target_dir}/.tdgpt-install-complete"
  local runtime_python
  local runtime_bin

  [ -f "$marker_file" ] || return 1
  [ -f "${target_dir}/pyvenv.cfg" ] || return 1
  [ -x "${target_dir}/bin/python3" ] || return 1
  [ "$(cat "$marker_file")" = "$(package_fingerprint "$package_path")" ] || return 1

  runtime_python=$(resolve_repair_python_bin) || return 1
  runtime_bin=$(dirname "$runtime_python")
  [ "$(readlink -f "${target_dir}/bin/python3")" = "$(readlink -f "$runtime_python")" ] || return 1
  grep -Fqx "home = ${runtime_bin}" "${target_dir}/pyvenv.cfg" || return 1
  grep -Fqx "executable = ${runtime_python}" "${target_dir}/pyvenv.cfg" || return 1
  grep -Fqx "command = ${runtime_python} -m venv ${target_dir}" "${target_dir}/pyvenv.cfg"
}

function validate_main_venv() {
  local target_dir="$1"
  local python_bin="${target_dir}/bin/python3"

  [ -f "${target_dir}/pyvenv.cfg" ] || {
    echo -e "\033[31mTDgpt venv package is missing pyvenv.cfg.\033[0m"
    return 1
  }
  [ -x "$python_bin" ] || {
    echo -e "\033[31mTDgpt venv package is missing an executable bin/python3.\033[0m"
    return 1
  }
  "$python_bin" -c 'import flask, gunicorn, numpy, pandas, scipy, sklearn' || {
    echo -e "\033[31mTDgpt venv package dependency validation failed.\033[0m"
    return 1
  }
}

function import_main_venv_package() {
  local package_path="$1"
  local target_dir="$2"
  local desc="$3"
  local tmp_dir
  local stage_dir
  local copy_root
  local entry_count
  local only_entry

  require_existing_path "$package_path" "$desc"
  cleanup_target_stage_dirs "$target_dir"

  if has_matching_venv_package_marker "$package_path" "$target_dir" && \
      validate_main_venv "$target_dir"; then
    echo -e "Reuse imported TDgpt virtual environment; package marker matches $(basename "$package_path")."
    return 0
  fi

  tmp_dir=$(create_target_stage_dir "$target_dir")
  stage_dir="${tmp_dir}/payload"
  ${csudo}mkdir -p "$stage_dir"

  if [ -d "$package_path" ]; then
    ${csudo}cp -a "${package_path}/." "$stage_dir/"
  else
    extract_archive_to_stage "$package_path" "$stage_dir" "$desc" "$tmp_dir"
  fi

  copy_root="$stage_dir"
  entry_count=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')
  if [ "$entry_count" = "1" ]; then
    only_entry=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 -print -quit)
    [ -d "$only_entry" ] && copy_root="$only_entry"
  fi

  if ! repair_venv_runtime_refs "$copy_root" "" "$target_dir" || ! validate_main_venv "$copy_root"; then
    ${csudo}rm -rf "$tmp_dir"
    echo -e "\033[31m${desc} validation failed; the existing virtual environment was not changed.\033[0m"
    return 1
  fi

  package_fingerprint "$package_path" | ${csudo}tee "${copy_root}/.tdgpt-install-complete" >/dev/null
  if ! replace_dir_from_stage "$copy_root" "$target_dir"; then
    ${csudo}rm -rf "$tmp_dir"
    return 1
  fi
  ${csudo}rm -rf "$tmp_dir"
}

function replace_dir_from_stage() {
  local source_dir="$1"
  local target_dir="$2"
  local target_parent
  local target_name
  local backup_target

  target_parent=$(dirname "$target_dir")
  target_name=$(basename "$target_dir")
  backup_target="${target_parent}/.${target_name}.apex-install-old"

  ${csudo}mkdir -p "$target_parent"

  # Recover the previous version if an earlier replacement stopped after the
  # old directory was renamed but before the staged directory was activated.
  if { [ ! -e "$target_dir" ] && [ ! -L "$target_dir" ]; } && { [ -e "$backup_target" ] || [ -L "$backup_target" ]; }; then
    ${csudo}mv "$backup_target" "$target_dir"
  elif [ -e "$backup_target" ] || [ -L "$backup_target" ]; then
    ${csudo}rm -rf "$backup_target"
  fi

  if [ -e "$target_dir" ] || [ -L "$target_dir" ]; then
    ${csudo}mv "$target_dir" "$backup_target"
  fi

  if ! ${csudo}mv "$source_dir" "$target_dir"; then
    if { [ -e "$backup_target" ] || [ -L "$backup_target" ]; } && { [ ! -e "$target_dir" ] && [ ! -L "$target_dir" ]; }; then
      ${csudo}mv "$backup_target" "$target_dir" || true
    fi
    echo -e "\033[31mFailed to activate staged directory for ${target_dir}.\033[0m"
    return 1
  fi

  ${csudo}rm -rf "$backup_target"
}

function extract_offline_package_to_dir() {
  local package_path="$1"
  local target_dir="$2"
  local desc="$3"
  local tmp_dir
  local stage_dir
  local copy_root
  local entry_count
  local only_entry

  require_existing_path "$package_path" "$desc"

  tmp_dir=$(create_target_stage_dir "$target_dir")
  stage_dir="${tmp_dir}/payload"
  ${csudo}mkdir -p "$stage_dir"

  if [ -d "$package_path" ]; then
    ${csudo}cp -a "${package_path}/." "$stage_dir/"
  else
    extract_archive_to_stage "$package_path" "$stage_dir" "$desc" "$tmp_dir"
  fi

  copy_root="$stage_dir"
  entry_count=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')
  if [ "$entry_count" = "1" ]; then
    only_entry=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 -print -quit)
    if [ -d "$only_entry" ]; then
      copy_root="$only_entry"
    fi
  fi

  if [ -z "$(${csudo}find "$copy_root" -mindepth 1 -print -quit)" ]; then
    ${csudo}rm -rf "$tmp_dir"
    echo -e "\033[31m${desc} package is empty after extraction: ${package_path}\033[0m"
    exit 1
  fi

  if ! replace_dir_from_stage "$copy_root" "$target_dir"; then
    ${csudo}rm -rf "$tmp_dir"
    exit 1
  fi
  ${csudo}rm -rf "$tmp_dir"
}

function extract_offline_package_children_to_dir() {
  local package_path="$1"
  local target_dir="$2"
  local desc="$3"
  local tmp_dir
  local stage_dir
  local copy_root
  local entry_count
  local only_entry
  local entry

  require_existing_path "$package_path" "$desc"

  tmp_dir=$(create_target_stage_dir "$target_dir")
  stage_dir="${tmp_dir}/payload"
  ${csudo}mkdir -p "$stage_dir"

  if [ -d "$package_path" ]; then
    ${csudo}cp -a "${package_path}/." "$stage_dir/"
  else
    extract_archive_to_stage "$package_path" "$stage_dir" "$desc" "$tmp_dir"
  fi

  copy_root="$stage_dir"
  entry_count=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')
  if [ "$entry_count" = "1" ]; then
    only_entry=$(${csudo}find "$stage_dir" -mindepth 1 -maxdepth 1 -print -quit)
    if [ -d "$only_entry" ]; then
      copy_root="$only_entry"
    fi
  fi

  if [ -z "$(${csudo}find "$copy_root" -mindepth 1 -print -quit)" ]; then
    ${csudo}rm -rf "$tmp_dir"
    echo -e "\033[31m${desc} package is empty after extraction: ${package_path}\033[0m"
    exit 1
  fi

  ${csudo}mkdir -p "$target_dir"
  while IFS= read -r entry; do
    if ! replace_dir_from_stage "$entry" "${target_dir}/$(basename "$entry")"; then
      ${csudo}rm -rf "$tmp_dir"
      exit 1
    fi
  done < <(${csudo}find "$copy_root" -mindepth 1 -maxdepth 1)

  ${csudo}rm -rf "$tmp_dir"
}

function extract_archive_to_stage() {
  local package_path="$1"
  local stage_dir="$2"
  local desc="$3"
  local tmp_dir="$4"
  local uncompressed_path

  case "$package_path" in
    *.tar.gz|*.tgz) ${csudo}tar -xzf "$package_path" -C "$stage_dir" ;;
    *.tar.xz|*.txz) ${csudo}tar -xJf "$package_path" -C "$stage_dir" ;;
    *.tar.zst|*.tzst)
      if tar --help 2>/dev/null | grep -q -- '--zstd'; then
        ${csudo}tar --zstd -xf "$package_path" -C "$stage_dir"
      elif command -v zstd >/dev/null 2>&1; then
        zstd -dc "$package_path" | ${csudo}tar -xf - -C "$stage_dir"
      elif command -v unzstd >/dev/null 2>&1; then
        unzstd -c "$package_path" | ${csudo}tar -xf - -C "$stage_dir"
      else
        ${csudo}rm -rf "$tmp_dir"
        echo -e "\033[31m${desc} requires zstd support, but tar --zstd/zstd/unzstd was not found: ${package_path}\033[0m"
        exit 1
      fi
      ;;
    *.tar) ${csudo}tar -xf "$package_path" -C "$stage_dir" ;;
    *.zip) ${csudo}unzip -q "$package_path" -d "$stage_dir" ;;
    *.zip.zst)
      uncompressed_path="${tmp_dir}/$(basename "${package_path%.zst}")"
      if command -v zstd >/dev/null 2>&1; then
        ${csudo}zstd -d -q -f -o "$uncompressed_path" "$package_path"
      elif command -v unzstd >/dev/null 2>&1; then
        ${csudo}unzstd -f -o "$uncompressed_path" "$package_path"
      else
        ${csudo}rm -rf "$tmp_dir"
        echo -e "\033[31m${desc} requires zstd support, but zstd/unzstd was not found: ${package_path}\033[0m"
        exit 1
      fi
      ${csudo}unzip -q "$uncompressed_path" -d "$stage_dir"
      ;;
    *)
      ${csudo}rm -rf "$tmp_dir"
      echo -e "\033[31mUnsupported ${desc} package format: ${package_path}\033[0m"
      exit 1
      ;;
  esac
}

function resolve_python_bin() {
  local candidate
  if [ -n "$python_home" ]; then
    for candidate in "${python_home}/bin/python3.${python_minor_ver}" "${python_home}/bin/python3" "${python_home}/bin/python"; do
      if [ -x "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  fi
  for candidate in \
    "$(command -v python3.${python_minor_ver} 2>/dev/null || true)" \
    "$(command -v python3 2>/dev/null || true)" \
    "$(command -v python 2>/dev/null || true)"; do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

function resolve_repair_python_bin() {
  local candidate
  if [ -n "$python_runtime_dir" ]; then
    for candidate in "${python_runtime_dir}/bin/python3.${python_minor_ver}" "${python_runtime_dir}/bin/python3" "${python_runtime_dir}/bin/python"; do
      if [ -x "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  fi
  resolve_python_bin
}

function repair_venv_runtime_refs() {
  local venv_path="$1"
  local runtime_python="${2:-}"
  local installed_venv_path="${3:-$venv_path}"
  local cfg_file="${venv_path}/pyvenv.cfg"
  local python3_link="${venv_path}/bin/python3"
  local runtime_bin

  if [ ! -f "${cfg_file}" ]; then
    echo "Warning: missing pyvenv.cfg for ${venv_path}"
    return 1
  fi

  if [ -z "$runtime_python" ]; then
    runtime_python=$(resolve_repair_python_bin) || {
      echo "Warning: python runtime not found for ${venv_path}"
      return 1
    }
  fi

  if [ ! -x "${runtime_python}" ]; then
    echo "Warning: runtime python not found: ${runtime_python}"
    return 1
  fi
  runtime_bin=$(dirname "${runtime_python}")

  sed -i -E \
    -e "s|^home = .*|home = ${runtime_bin}|" \
    -e "s|^executable = .*|executable = ${runtime_python}|" \
    -e "s|^command = .*|command = ${runtime_python} -m venv ${installed_venv_path}|" \
    "${cfg_file}"

  if [ -e "${python3_link}" ] || [ -L "${python3_link}" ]; then
    ${csudo}rm -f "${python3_link}"
  fi
  ${csudo}ln -s "${runtime_python}" "${python3_link}"
}

function repair_existing_model_venvs() {
  local venv_path
  local repaired=0
  for venv_path in "${timesfm_venv_dir}" "${moirai_venv_dir}" "${chronos_venv_dir}" "${momentfm_venv_dir}"; do
    if [ -d "${venv_path}" ]; then
      repair_venv_runtime_refs "${venv_path}"
      repaired=1
    fi
  done
  if [ $repaired -eq 0 ]; then
    echo -e "\033[31mTDgpt model venv package did not create any recognized model virtual environments.\033[0m"
    exit 1
  fi
}

function configure_service_python_runtime() {
  local service_script="${installDir}/bin/taosanode_service.py"
  local runtime_python

  # Keep direct execution aligned with an explicitly supplied Python runtime.
  [ -n "${python_runtime_dir}" ] || return 0
  runtime_python=$(resolve_repair_python_bin) || {
    echo -e "\033[31mPython runtime not found for service entry point.\033[0m"
    exit 1
  }
  [ -f "${service_script}" ] || {
    echo -e "\033[31mService entry point not found: ${service_script}\033[0m"
    exit 1
  }

  ${csudo}sed -i -E "1c\\#!${runtime_python}" "${service_script}"
}

function install_anode_venv() {
  ${csudo}mkdir -p "$(dirname "${venvDir}")" && ${csudo}chmod 777 "$(dirname "${venvDir}")"
  if [ ${custom_dir_set} -eq 0 ]; then
    ${csudo}ln -sfn "${venvDir}" "${install_main_dir}/venv"
  fi
  if [ -n "$venv_package" ]; then
    echo -e "Import TDgpt virtual environment package: ${venv_package}"
    import_main_venv_package "$venv_package" "$venvDir" "TDgpt venv package"
    echo -e "TDgpt virtual environment package imported to ${venvDir}"
    return 0
  fi
  if [ ${install_venv} == "True" ]; then
    # build venv
    python_cmd=$(resolve_python_bin) || {
      echo -e "\033[31mPython runtime not found for main venv creation.\033[0m"
      exit 1
    }
    "${python_cmd}" -m venv "${venvDir}"

    echo -e "active Python3 virtual env: ${venvDir}"
    source "${venvDir}/bin/activate"
    requirements_file="requirements_ess.txt"
    if [ "${lite_mode}" == "1" ]; then
      requirements_file="requirements_docker.txt"
    fi
    echo -e "install the required packages by pip3, this may take a while depending on the network condition"
    req_file=$(resolve_requirement_file "${requirements_file}") || return 1
    run_venv_pip install -r "${req_file}" "${pip_extra_args[@]}"
    repair_venv_runtime_refs "${venvDir}"

    echo -e "Install python library for venv completed!"
  else
    echo -e "Install python library for venv skipped!"
  fi
}

function install_extra_venvs() {
  ${csudo}mkdir -p "${dataDir}" && ${csudo}chmod 777 "${dataDir}"
  if [ -n "$model_venv_package" ]; then
    echo -e "Import TDgpt model virtual environment package: ${model_venv_package}"
    extract_offline_package_children_to_dir "$model_venv_package" "$dataDir" "TDgpt model venv package"
    repair_existing_model_venvs
    echo -e "TDgpt model virtual environment package imported to ${dataDir}"
    return 0
  fi
  if [ ! -f "${venvDir}/bin/pip3" ]; then
    echo -e "${RED}Error: Virtual environment not found at ${venvDir}. Cannot proceed with TSFM dependencies installation.${NC}"
    return 1
  fi

  echo -e "${GREEN}Installing TSFM dependencies into unified venv at ${venvDir}${NC}"
  req_file=$(resolve_requirement_file requirements_tsfm.txt) || return 1
  run_venv_pip install -r "${req_file}" "${pip_extra_args[@]}"

  echo -e "${GREEN}Installing momentfm with --no-deps${NC}"
  run_venv_pip install --no-deps momentfm==0.1.4 "${pip_extra_args[@]}"

  if ! pip_check_output=$(run_venv_pip check 2>&1); then
    echo -e "${RED}Error: pip dependency check reported issues:${NC}"
    echo "${pip_check_output}"
    echo -e "${RED}Error: Python dependencies are inconsistent; installation cannot continue.${NC}"
    return 1
  fi
  echo -e "${GREEN}Unified TSFM dependencies installed.${NC}"
}

function clean_service_on_sysvinit() {
  if ps aux | grep -v grep | grep $1 &>/dev/null; then
    ${csudo}service $1 stop || :
  fi

  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}chkconfig --del $1 || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}insserv -r $1 || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/$1 ]; then
      ${csudo}update-rc.d -f $1 remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/$1 || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

function install_service_on_sysvinit() {
  if [ "$1" != "${serverName}" ]; then
    return
  fi

  clean_service_on_sysvinit $1
  sleep 1

  if ((${os_type} == 1)); then
    ${csudo}cp ${script_dir}/init.d/${serverName}.deb ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  elif ((${os_type} == 2)); then
    ${csudo}cp ${script_dir}/init.d/${serverName}.rpm ${service_config_dir}/${serverName} && ${csudo}chmod a+x ${service_config_dir}/${serverName}
  fi

  if ((${initd_mod} == 1)); then
    ${csudo}chkconfig --add $1 || :
    ${csudo}chkconfig --level 2345 $1 on || :
  elif ((${initd_mod} == 2)); then
    ${csudo}insserv $1} || :
    ${csudo}insserv -d $1 || :
  elif ((${initd_mod} == 3)); then
    ${csudo}update-rc.d $1 defaults || :
  fi
}

function clean_service_on_systemd() {
  service_config="${service_config_dir}/$1.service"

  if systemctl is-active --quiet $1; then
    echo "$1 is running, stopping it..."
    ${csudo}systemctl stop $1 &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable $1 &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${service_config}
}

function install_service_on_systemd() {
  clean_service_on_systemd $1

  cfg_source_dir=${script_dir}/cfg
  
  if [ -f "${cfg_source_dir}/$1.service" ]; then
    sed -i.bak \
        -e "s|/usr/local/taos/taosanode|${install_main_dir}|g" \
        -e "s|/var/lib/taos/taosanode|${dataDir}|g" \
        "${cfg_source_dir}/$1.service"
    rm -rf "${cfg_source_dir}/$1.service.bak" || :
    ${csudo}cp "${cfg_source_dir}/$1.service" "${service_config_dir}/" || :
  fi

  ${csudo}systemctl enable $1
  ${csudo}systemctl daemon-reload
}

function is_container() {
  if [[ -f /.dockerenv ]] || grep -q "docker\|kubepods" /proc/1/cgroup || [[ -n "$KUBERNETES_SERVICE_HOST" || "$container" == "docker" ]]; then
    return 0  # container env
  else
    return 1  # not container env
  fi
}

function install_service() {
  if ((${service_mod} == 0)); then
    install_service_on_systemd $1
  elif ((${service_mod} == 1)); then
    install_service_on_sysvinit $1
  else
    kill_process $1
  fi
}

function installProduct() {
  # Start to install
  if [ ! -e ${tarName} ]; then
    echo "File ${tarName} does not exist"
    exit 1
  fi

  tar -zxf ${tarName}
 
  echo "Start to install ${productName}..."
  validate_offline_packages

  install_main_path
  install_log
  install_anode_config
  install_model_dirs
  if [ "${lite_mode}" != "1" ]; then
    install_model
  fi
  install_img
  install_resource
  
  install_bin_and_lib

  echo
  echo -e "\033[44;32;1mStart to create virtual python env in ${venvDir}${NC}"
  install_anode_venv
  configure_service_python_runtime

  if [ ${all_venv} -eq 1 ] || [ -n "$model_venv_package" ]; then
    echo -e "\033[44;32;1mStart to create extra venvs for chronos-forecasting and momentfm${NC}"
    install_extra_venvs
  fi

  kill_model_service

  if ! is_container; then
    install_services
  fi

  echo
  echo -e "\033[44;32;1m${productName} is installed successfully!${NC}"
  write_install_state
}

# check for python version, only the 3.10/3.11/3.12 is supported
check_python3_env() {
  if [ -n "$python_home" ]; then
    python_cmd=$(resolve_python_bin) || {
      echo -e "\033[31mWarning: Python home does not contain a usable python binary: ${python_home}\033[0m"
      exit 1
    }
    python3_version=$("$python_cmd" --version 2>&1 | awk -F' ' '{print $2}')
    python_minor_ver=$(echo "$python3_version" | cut -d"." -f2)
    if [[ $(echo "$python3_version" | cut -d"." -f1) -eq 3 && $python_minor_ver =~ ^(10|11|12)$ ]]; then
      echo -e "\033[32mPython ${python3_version} has been found in ${python_home}.\033[0m"
      return 0
    fi
    echo -e "\033[31mWarning: Python home must provide Python 3.10/3.11/3.12, found ${python3_version}.\033[0m"
    exit 1
  fi

  if ! command -v python3 &> /dev/null
  then
      echo -e "\033[31mWarning: Python3 command not found. Version 3.10/3.11 is required.\033[0m"
      exit 1
  fi

  python3_version=$(python3 --version 2>&1 | awk -F' ' '{print $2}')

  python3_version_ok=false
  python_minor_ver=$(echo "$python3_version" | cut -d"." -f2)
  if [[ $(echo "$python3_version" | cut -d"." -f1) -eq 3 && $python_minor_ver =~ ^(10|11|12)$ ]]; then
    python3_version_ok=true
  fi

  if $python3_version_ok; then
    echo -e "\033[32mPython ${python3_version} has been found.\033[0m"
  else
    if command -v python3.10 &> /dev/null
    then
      echo -e "\033[32mPython3.10 has been found.\033[0m"
      python_minor_ver=10
    elif command -v python3.11 &> /dev/null
    then
      python_minor_ver=11
      echo -e "\033[32mPython3.11 has been found.\033[0m"
    elif command -v python3.12 &> /dev/null
    then
      python_minor_ver=12
      echo -e "\033[32mPython3.12 has been found.\033[0m"

    else
      echo -e "\033[31mWarning: Python3.10/3.11/3.12 allowed, only found python${python3_version}.\033[0m"
      exit 1
    fi
  fi

#  echo -e "Python3 minor version is:${python_minor_ver}"

  # check the existence pip3.10/pip3.11/pip3.12
  if ! command -v pip3 &> /dev/null
  then
    echo -e "\033[31mWarning: Pip3 command not found. Version 3.10/3.11 allowed.\033[0m"
    exit 1
  fi

  pip3_version=$(pip3 --version 2>&1 | awk -F' ' '{print $6}' | cut -d")" -f1)
  major_ver=$(echo "${pip3_version}" | cut -d"." -f1)
  minor_ver=$(echo "${pip3_version}" | cut -d"." -f2)

  pip3_version_ok=false;
  if [[ ${major_ver} -eq 3 && ${minor_ver} -ge 10 ]]; then
    pip3_version_ok=true
  fi

  if $pip3_version_ok; then
    echo -e "\033[32mpip3 ${pip3_version} has been found.\033[0m"
  else
    if command -v pip3.${python_minor_ver} &> /dev/null
    then
      echo -e "\033[32mpip3.${python_minor_ver} has been found.\033[0m"
    else
      echo -e "\033[31mWarning: pip3.10/3.11/3.12 allowed, only found pip${pip3_version}.\033[0m"
     exit 1
    fi
  fi

#  if ! command -v python3.${python_minor_ver}-venv &> /dev/null
#  then
#    echo -e "\033[31mWarning: python3.${python_minor_ver}-venv command not found.\033[0m"
#    exit 1
#  fi
}

## ==============================Main program starts from here============================
serverFqdn=$(hostname)
detect_requested_install_mode
guard_install_mode_transition

if [ "$verType" == "server" ]; then
    if [ -n "$venv_package" ] && { [ -n "$model_venv_package" ] || [ ${all_venv} -eq 0 ]; }; then
      echo -e "\033[32mOffline venv package provided, skip system Python/pip environment check.\033[0m"
    else
    check_python3_env
    fi
    installProduct
fi
