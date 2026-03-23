#!/bin/bash
# Unified model service starter for tdtsfm, timemoe, chronos, moirai, etc.
# Usage: start-model.sh [-c config_file] [model_name|all] [extra args...]

show_help() {
  echo "Usage: $0 [-c config_file] [model_name|all] [extra args...]"
  echo "Supported model_name: tdtsfm, timesfm, timemoe, moirai, chronos, moment"
  echo "Use '$0 all' to start all models in background."
  echo "Options:"
  echo "  -c config_file   Specify config file (default: /usr/local/taos/taosanode/cfg/taosanode.config.py)"
  echo "  -h, --help       Show this help message"
}

CONFIG_FILE="${TAOSANODE_CONFIG:-/usr/local/taos/taosanode/cfg/taosanode.config.py}"

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
  ARGS=("$@")
}

# Read a variable from the Python config file (taosanode.config.py)
py_cfg_get() {
  local key="$1" default="$2"
  local val
  val=$(python3 -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location('cfg', '${CONFIG_FILE}')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
val = getattr(mod, '${key}', None)
if val is not None:
    print(val)
else:
    print('${default}')
" 2>/dev/null) || val="$default"
  printf '%s\n' "${val:-$default}"
}

main() {
  # parse options
  parse_options "$@"
  set -- "${ARGS[@]}"

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

  # Load config from taosanode.config.py
  model_dir="$(py_cfg_get model_dir "/var/lib/taos/taosanode/model")"
  pythonpath="$(py_cfg_get pythonpath "/usr/local/taos/taosanode/lib/taosanalytics/")"
  # service_dir is the tsfmservice subdirectory under pythonpath
  service_dir="${pythonpath%/}/tsfmservice"
  default_venv="$(py_cfg_get virtualenv "/var/lib/taos/taosanode/venv")"
  timesfm_venv="$(py_cfg_get timesfm_venv "/var/lib/taos/taosanode/timesfm_venv")"
  moirai_venv="$(py_cfg_get moirai_venv "/var/lib/taos/taosanode/moirai_venv")"
  chronos_venv="$(py_cfg_get chronos_venv "/var/lib/taos/taosanode/chronos_venv")"
  moment_venv="$(py_cfg_get momentfm_venv "/var/lib/taos/taosanode/momentfm_venv")"
  errorlog="$(py_cfg_get errorlog "/var/log/taos/taosanode/error.log")"
  log_dir="$(dirname "$errorlog")"
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
