#!/bin/bash
# Unified model service stopper for tdtsfm, timesfm, timemoe, moirai, chronos, moment
# Usage: stop-model.sh [model_name|all]

show_help() {
  echo "Usage: $0 [model_name|all]"
  echo "Supported model_name: tdtsfm, timesfm, timemoe, moirai, chronos, moment"
  echo "Use '$0 all' to stop all models."
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help
  exit 0
fi

MODELS=(tdtsfm timesfm timemoe moirai chronos moment)

stop_model() {
  local model="$1"
  local script_file="${model}-server.py"
  local pid
  pid=$(pgrep -f "${script_file}")
  if [ -n "$pid" ]; then
    echo "Stopping ${script_file} with PID $pid"
    kill -9 $pid
    echo "${script_file} stopped"
  else
    echo "${script_file} is not running"
  fi
}

if [[ "$1" == "all" ]]; then
  for m in "${MODELS[@]}"; do
    stop_model "$m"
    echo
  done
  echo "All models stopped."
  exit 0
elif [[ -z "$1" ]]; then
  show_help
  exit 1
else
  found=0
  for m in "${MODELS[@]}"; do
    if [[ "$1" == "$m" ]]; then
      stop_model "$m"
      found=1
      break
    fi
  done
  if [[ $found -eq 0 ]]; then
    echo "Unknown model: $1"
    show_help
    exit 1
  fi
fi