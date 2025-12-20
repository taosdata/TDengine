#!/bin/bash
# Unified model service starter for tdtsfm, timemoe, chronos, moirai, etc.
# Usage: start-model.sh [model_name] [extra args...]

# --- INI parser (原ini_utils.sh内容) ---
CONFIG_FILE="${TAOSANODE_CONFIG:-/etc/taos/taosanode.ini}"
parse_args() {
  while getopts ":c:h" opt; do
    case "$opt" in
      c) CONFIG_FILE="$OPTARG" ;;
      h) echo "usage: $0 [-c config_file_path]"; exit 0 ;;
      *) echo "Invalid option: -$OPTARG" >&2; exit 2 ;;
    esac
  done
}
ini_get() {
  local section="$1" key="$2" default="${3-}" file="$CONFIG_FILE"
  local in_section=0 line k v
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%\#*}"; line="${line%%;*}"
    line="$(printf '%s' "$line" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//')"
    [ -z "$line" ] && continue
    if [[ "$line" =~ ^\[(.+)\]$ ]]; then
      [[ "${BASH_REMATCH[1]}" == "$section" ]] && in_section=1 || in_section=0
      continue
    fi
    if [[ $in_section -eq 1 && "$line" =~ ^([^=]+)=[[:space:]]*(.*)$ ]]; then
      k="$(printf '%s' "${BASH_REMATCH[1]}" | sed 's/[[:space:]]//g')"
      v="${BASH_REMATCH[2]}"
      v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
      if [[ "$k" == "$key" ]]; then
        printf '%s\n' "$v"
        return 0
      fi
    fi
  done < "$file"
  printf '%s\n' "$default"
}
# --- END INI parser ---

parse_args "$@"
shift $((OPTIND-1))

model_name="$1"
shift

model_dir="$(ini_get taosanode model-dir "/var/lib/taos/taosanode/model")"
lib_base="$(ini_get uwsgi chdir "/usr/local/taos/taosanode/lib")"
service_dir="${lib_base%/}/taosanalytics/tsfmservice"
default_4_44_venv_dir="$(ini_get uwsgi virtualenv "/var/lib/taos/taosanode/venv")"
chronos_4_55_venv_dir="$(ini_get uwsgi chronos_venv_dir "/var/lib/taos/taosanode/venv_chronos")"
moment_4_33_venv_dir="$(ini_get uwsgi momentfm_venv_dir  "/var/lib/taos/taosanode/venv_momentfm")"

case "$model_name" in
  tdtsfm)
    venv_dir="$default_4_44_venv_dir"
    sub_model_dir="${model_dir%/}/tdtsfm"
    py_script="tdtsfm-server.py"
    model_param=""
    py_args="--action server $*"
    ;;
  timesfm)
    venv_dir="$default_4_44_venv_dir"
    sub_model_dir="${model_dir%/}/timesfm"
    py_script="timemoe-server.py"
    model_param="Maple728/TimeMoE-200M"
    py_args="${sub_model_dir} ${model_param} False $*"
    ;;    
  timemoe)
    venv_dir="$default_4_44_venv_dir"
    sub_model_dir="${model_dir%/}/timemoe"
    py_script="timemoe-server.py"
    model_param="Maple728/TimeMoE-200M"
    py_args="${sub_model_dir} ${model_param} False $*"
    ;;
  moirai)
    venv_dir="$default_4_44_venv_dir"
    sub_model_dir="${model_dir%/}/moirai"
    py_script="moirai-server.py"
    model_param="Salesforce/moirai-moe-1.0-R-base"
    py_args="${sub_model_dir} ${model_param} False $*"
    ;;
  chronos)
    venv_dir="$chronos_4_55_venv_dir"
    sub_model_dir="${model_dir%/}/chronos"
    py_script="chronos-server.py"
    model_param="amazon/chronos-bolt-base"
    py_args="${sub_model_dir} ${model_param} False $*"
    ;;
  moment)
    venv_dir="$moment_4_33_venv_dir"
    sub_model_dir="${model_dir%/}/moment-large"
    py_script="moment-server.py"
    model_param="AutonLab/MOMENT-1-large"
    py_args="${sub_model_dir} ${model_param} False $*"
    ;;
  *)
    echo "Usage: $0 [tdtsfm|timesfm|timemoe|moirai|chronos|moment] [extra args...]"
    exit 1
    ;;
esac

# ...existing code...

if [ -d "${sub_model_dir}" ]; then
    echo "Directory ${sub_model_dir} exists."
    echo "Starting ${model_name} service"
    cd "${service_dir}" || exit
    source "${venv_dir}/bin/activate"
    nohup python3 ${py_script} ${py_args} &
    echo "check the pid of the ${py_script} to confirm it is running"
    pid=$(pgrep -f "${py_script}")
    echo "PID of the ${py_script} is $pid"
else
    echo "Directory ${sub_model_dir} does not exist."
    exit 1
fi