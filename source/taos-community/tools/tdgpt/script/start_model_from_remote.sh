#!/bin/bash

# load and start tsfm from the huggingface.com or mirror site.
set -e

while getopts "e:n:d:p:s:" opt; do
    case "$opt" in
        e) env="$OPTARG" ;;  # -e virtual env path
        n) model_name="$OPTARG" ;;  # -n model full name
        d) model_dir="$OPTARG" ;;  # -d model directory
        p) proxy="$OPTARG" ;; # -p enable proxy or not
        s) service="$OPTARG" ;; # -s service name
        *) echo "Usage: $0 -e env -n model_name -d model_dir -s service_name -p enable_proxy"; exit 1 ;;
    esac
done

if [ -z "$env" ] || [ -z "$model_name" ] || [ -z "$model_dir" ] || [ -z "$service" ]; then
    echo "Usage: $0 -e env -n model_name -d model_dir -s service_name -p enable_proxy"
    exit 1
fi

echo "start to load model from remote site"
echo "venv path:  ${env} "
echo "model_name: ${model_name}"
echo "model directory: ${model_dir}"
echo "service_name: ${service}"

curr_dir=$(pwd)
echo -e ${curr_dir}
cd ${curr_dir}

function active_venv() {
  echo -e "active Python3 virtual env: ${env}"

  if [ ! -d "${env}/bin/" ]; then
    echo "warning：venv ${env}/bin does not exist, quit"
  fi

  # active the venv
  source ${env}/bin/activate

  echo -e "Active Python3 venv completed!"
}

scriptRoot="/usr/local/taos/taosanode/lib/taosanalytics/tsfmservice/"

function download_start_model() {
  echo -e "Start to download ${model_name} into ${model_dir} and start tsfm service"

  cd "${scriptRoot}" || {
      echo "ERROR: failed to switch to directory: ${scriptRoot}"
      exit 1
  }

  echo -e "python3.10 ${service}.py ${model_dir} ${model_name} ${proxy}"
  python3.10 ${service}.py ${model_dir} ${model_name} ${proxy}
}

active_venv
download_start_model