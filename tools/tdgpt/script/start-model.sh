#!/bin/bash
# Unified model service starter for tdtsfm, timemoe, chronos, moirai, etc.
# Usage: start-model.sh [-c config_file] [model_name|all] [extra args...]

show_help() {
  echo "Usage: $0 [-c config_file] [model_name|all] [extra args...]"
  echo "Supported model_name: tdtsfm, timesfm, timemoe, moirai, chronos, moment"
  echo "Use '$0 all' to start all models in background."
  echo "Options:"
  echo "  -c config_file   Specify config file (default: /etc/taos/taosanode.ini)"
  echo "  -h, --help       Show this help message"
}

CONFIG_FILE="${TAOSANODE_CONFIG:-/etc/taos/taosanode.ini}"

# Parse command-line options
parse_options() {
  while getopts ":c:h" opt; do
    case "$opt" in
      c) CONFIG_FILE="$OPTARG" ;;
      h) show_help; exit 0 ;;
      *) echo "Invalid option: -$OPTARG" >&2; exit 2 ;;
    esac
  done
  shift $((OPTIND-1))
  # return remaining args
  echo "$@"
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

main() {
  # parse options
  local args
  args=$(parse_options "$@")
  set -- $args

  if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
  fi

  if [[ "$1" == "all" ]]; then
    shift
    for m in tdtsfm timesfm timemoe moirai chronos moment; do
      (
        "$0" -c "$CONFIG_FILE" "$m" "$@" | sed "s/^/[$m] /"
      ) &
      sleep 1
      echo
    done
    echo "All models started."
    exit 0
  fi

  model_name="$1"
  shift

  # Load config
  model_dir="$(ini_get taosanode model-dir "/var/lib/taos/taosanode/model")"
  lib_base="$(ini_get uwsgi chdir "/usr/local/taos/taosanode/lib")"
  service_dir="${lib_base%/}/taosanalytics/tsfmservice"
  default_venv="$(ini_get uwsgi virtualenv "/var/lib/taos/taosanode/venv")"
  timesfm_venv="$(ini_get uwsgi timesfm_venv "/var/lib/taos/taosanode/timesfm_venv")"
  moirai_venv="$(ini_get uwsgi moirai_venv "/var/lib/taos/taosanode/moirai_venv")"
  chronos_venv="$(ini_get uwsgi chronos_venv "/var/lib/taos/taosanode/chronos_venv")"
  moment_venv="$(ini_get uwsgi momentfm_venv  "/var/lib/taos/taosanode/momentfm_venv")"
  logto="$(ini_get uwsgi logto "/var/log/taos/taosanode/taosanode.log")"
  log_dir="$(dirname "$logto")"
  service_log="${log_dir%/}/taosanode_service_${model_name}.log"

  # Model config
  case "$model_name" in
    tdtsfm)
      venv_dir="${default_venv}"
      sub_model_dir="${model_dir%/}/tdtsfm"
      py_script="tdtsfm-server.py"
      set -- "${sub_model_dir}" "--action" "server" "$@"
      ;;
    timesfm)
      venv_dir="${timesfm_venv}"
      sub_model_dir="${model_dir%/}/timesfm"
      py_script="timesfm-server.py"
      model_param="google/timesfm-2.0-500m-pytorch"
      set -- "${sub_model_dir}" "${model_param}" False "$@"
      ;;
    timemoe)
      venv_dir="${default_venv}"
      sub_model_dir="${model_dir%/}/timemoe"
      py_script="timemoe-server.py"
      model_param="Maple728/TimeMoE-200M"
      set -- "${sub_model_dir}" "${model_param}" False "$@"
      ;;
    moirai)
      venv_dir="${moirai_venv}"
      sub_model_dir="${model_dir%/}/moirai"
      py_script="moirai-server.py"
      model_param="Salesforce/moirai-moe-1.0-R-base"
      set -- "${sub_model_dir}" "${model_param}" False "$@"
      ;;
    chronos)
      venv_dir="${chronos_venv}"
      sub_model_dir="${model_dir%/}/chronos"
      py_script="chronos-server.py"
      model_param="amazon/chronos-bolt-base"
      set -- "${sub_model_dir}" "${model_param}" False "$@"
      ;;
    moment)
      venv_dir="${moment_venv}"
      sub_model_dir="${model_dir%/}/moment-large"
      py_script="moment-server.py"
      model_param="AutonLab/MOMENT-1-large"
      set -- "${sub_model_dir}" "${model_param}" False "$@"
      ;;
    *)
      show_help
      exit 1
      ;;
  esac

  # Start model
  if [ -d "${sub_model_dir}" ]; then
    echo "Directory ${sub_model_dir} exists."
    echo "Starting ${model_name} service"
    cd "${service_dir}" || exit
    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"
    echo "nohup ${venv_dir}/bin/python3 ${py_script} $* >> ${service_log} 2>&1 &"
    nohup "${venv_dir}/bin/python3" "${py_script}" "$@" >> "${service_log}" 2>&1 &
    echo "check the pid of the ${py_script} to confirm it is running"
    pid=""
    for _ in {1..10}; do
      pid=$(pgrep -f "${py_script}")
      if [ -n "$pid" ]; then
        break
      fi
      sleep 1
    done
    if [ -n "$pid" ]; then
      echo "PID of the ${py_script} is $pid"
    else
      echo "PID of the ${py_script} not found after waiting, it may still be starting or failed."
    fi
  else
    echo "Directory ${sub_model_dir} does not exist."
    exit 1
  fi
}

main "$@"