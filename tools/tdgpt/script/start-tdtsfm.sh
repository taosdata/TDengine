#!/bin/bash
# parse taosanode.ini  for path
# shellcheck source=/dev/null
SCRIPT_PATH="$(readlink -f "$0")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
source "${SCRIPT_DIR}/ini_utils.sh"
parse_args "$@"

model_dir="$(ini_get taosanode model-dir "/var/lib/taos/taosanode/model")"
venv_dir="$(ini_get uwsgi virtualenv "/var/lib/taos/taosanode/venv")"
lib_base="$(ini_get uwsgi chdir "/usr/local/taos/taosanode/lib")"
service_dir="${lib_base%/}/taosanalytics/tsfmservice"

# set path 
tdtsfm_model_file="${model_dir%/}/tdtsfm"
if [ -d "${tdtsfm_model_file}" ]; then
    echo "Directory ${tdtsfm_model_file} exists."
    echo "Starting tdtsfm service"
    cd "${service_dir}" || exit
    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"
    nohup python3 tdtsfm-server.py --action server &
    echo "check the pid of the tdtsfm-server.py to confirm it is running"
    pid=$(pgrep -f "tdtsfm-server.py")
    echo "PID of the tdtsfm-server.py is $pid"
else
    echo "Directory ${tdtsfm_model_file} does not exist."
    exit 1
fi