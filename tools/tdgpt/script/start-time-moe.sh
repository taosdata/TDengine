#!/bin/bash
# Parse taosanode.ini for all paths
# shellcheck source=/dev/null
SCRIPT_PATH="$(readlink -f "$0")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
source "${SCRIPT_DIR}/ini_utils.sh"
parse_args "$@"

model_dir="$(ini_get taosanode model-dir "/var/lib/taos/taosanode/model")"
venv_dir="$(ini_get uwsgi virtualenv "/var/lib/taos/taosanode/venv")"
lib_base="$(ini_get uwsgi chdir "/usr/local/taos/taosanode/lib")"
service_dir="${lib_base%/}/taosanalytics/tsfmservice"

timemoe_model_dir="${model_dir%/}/timemoe"

if [ -d "${timemoe_model_dir}" ]; then
    echo "Directory ${timemoe_model_dir} exists."
    echo "Starting timemoe service"
    cd "${service_dir}" || exit
    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"
    nohup python3 timemoe-server.py "${timemoe_model_dir}" Maple728/TimeMoE-200M False &
    echo "check the pid of the timemoe-server.py to confirm it is running"
    pid=$(pgrep -f "timemoe-server.py")
    echo "PID of the timemoe-server.py is $pid"
else
    echo "Directory ${timemoe_model_dir} does not exist."
    exit 1
fi