#!/bin/bash

# docker run -d --name tdgpt -p 6090:6090 -p 5000:5000 -p 5001:5001 tdengine/tdengine-tdgpt-full:3.3.6.0

export PATH="/var/lib/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8

# Define the base path for models
MODEL_BASE_PATH="/var/lib/taos/taosanode/model"
MODEL_DIR_NAMES="${TAOS_MODELS:-tdtsfm}"
if [ -z "$EP_ENABLE" ]; then
    ENDPOINT_ENABLE="False"
elif [ "$EP_ENABLE" == "True" ]; then
    ENDPOINT_ENABLE="True"
else
    ENDPOINT_ENABLE="False"
fi

# Define the five subdirectories under model path
declare -A MODEL_SUBDIRS=(
    ["chronos"]="model.safetensors"
    ["moirai"]="model.safetensors"
    ["tdtsfm"]="taos.pth"
    ["timemoe"]="model.safetensors"
    ["timesfm"]="model.safetensors"
    ["moment"]="model.safetensors"
)
declare -A MODEL_NAMES=(
    ["chronos"]="amazon/chronos-bolt-tiny"
    ["moirai"]="Salesforce/moirai-moe-1.0-R-small"
    ["tdtsfm"]="tdtsfm"
    ["timemoe"]="Maple728/TimeMoE-200M"
    ["timesfm"]="google/timesfm-2.0-500m-pytorch"
    ["moment"]="AutonLab/MOMENT-1-base"
)

declare -A MODEL_VENV_MAP=(
    ["chronos"]="/var/lib/taos/taosanode/venv_chronos"
    ["moirai"]="/var/lib/taos/taosanode/venv"
    ["tdtsfm"]="/var/lib/taos/taosanode/venv"
    ["timemoe"]="/var/lib/taos/taosanode/venv"
    ["timesfm"]="/var/lib/taos/taosanode/venv_timesfm"
    # use the same venv as timesfm
    ["moment"]="/var/lib/taos/taosanode/venv_timesfm"
)

# Function to activate virtual environment
activate_venv() {
    local model="$1"
    echo "Activating virtual environment..."
    local venv_path="${MODEL_VENV_MAP[$model]}"
    echo "venv path: ${venv_path}"
    # shellcheck disable=SC1091
    source "${venv_path}/bin/activate"
}

# Function to execute startup script
execute_startup() {
    local subdir="$1"
    local model="$2"
    local model_name="$3"

    echo "Executing startup script for $subdir..."

    # Activate virtual environment
    activate_venv "${model}"

    # Execute startup script if exists
    local script_path="/usr/local/taos/taosanode/lib/taosanalytics/tsfmservice"
    local startup_script="${script_path}/${model}-server.py"
    if [ -f "${startup_script}" ]; then
        echo "Running startup script: ${startup_script}"
        cd "${script_path}" || exit
        echo "Current directory: $(pwd)"
        if [ "${model}" == "moment" ]; then
            echo "modifying moment-server.py model list"
            sed -i "s|_model_list\[0\]|'${subdir}'|g" "${model}-server.py" && echo "${model}-server.py updated"
        fi
        if [ "${model}" == "tdtsfm" ] || [ "${model}" == "moment" ]; then
            echo "Running command: nohup python3 /usr/local/taos/taosanode/lib/taosanalytics/tsfmservice/${model}-server.py &"
            nohup python3 "${model}-server.py" --action server &
        else
            echo "Running command: nohup python3 ${model}-server.py ${subdir} ${model_name} ${ENDPOINT_ENABLE} &"
            nohup python3 "${model}-server.py" "${subdir}" "${model_name}" "${ENDPOINT_ENABLE}" &
        fi
        SERVER_PID=$!
        if ps -p ${SERVER_PID} > /dev/null; then
            echo "Startup script executed successfully for ${subdir}"
        else
            echo "Error: Startup script failed for ${subdir}"
            exit 1
        fi
    else
        echo "Startup script not found: ${startup_script}"
    fi
}

# Function to download and setup
download_and_setup() {
    local subdir="$1"
    local model_name="$2"
    local flag_file="$3"

    echo "Downloading and setting up ${subdir}..."

    # Execute download script if exists
    local download_script="/usr/local/taos/taosanode/lib/taosanalytics/misc/model_downloader.py"
    if [ -f "${download_script}" ]; then
        echo "Running download script: ${download_script}"
        cd /usr/local/taos/taosanode/lib/taosanalytics/misc || exit
        echo "Current directory: $(pwd)"
        echo "Running command: python3 model_downloader.py ${subdir} ${model_name} ${ENDPOINT_ENABLE}"
        python3 model_downloader.py "${subdir}" "${model_name}" "${ENDPOINT_ENABLE}"
    else
        echo "Download script not found: ${download_script}"
    fi

    # Verify file was downloaded/created
    if [ -f "${subdir}/${flag_file}" ]; then
        echo "Successfully downloaded ${model_name}"
    else
        echo "Error: download Failed for ${subdir} "
        exit 1
    fi
}

find_model_file() {
    local model="$1"
    local base_path="${MODEL_BASE_PATH}/${model}"
    local flag_file="${MODEL_SUBDIRS[$model]}"
    
    # Priority 1: Check the root directory
    if [ -f "${base_path}/${flag_file}" ] || [ -L "${base_path}/${flag_file}" ]; then
        echo "${base_path}"
        return 0
    fi
    
    # Priority 2: Check the snapshots directory (HuggingFace cache format)
    local snapshot_dir
    snapshot_dir=$(find "${base_path}/snapshots" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -n 1)
    if [ -n "${snapshot_dir}" ] && [ -e "${snapshot_dir}/${flag_file}" ]; then
        echo "${snapshot_dir}"
        return 0
    fi
    
    # Priority 3: Check the blobs directory
    local blob_file
    blob_file=$(find "${base_path}/blobs" -name "*${flag_file}*" 2>/dev/null | head -n 1)
    if [ -n "${blob_file}" ]; then
        dirname "${blob_file}"
        return 0
    fi
    
    return 1
}

# Check each directory and handle missing files
if mount | grep -q "${MODEL_BASE_PATH}"; then
    echo "Model base path ${MODEL_BASE_PATH} is mounted."
    # 检查 MODEL_DIR_NAMES 是否为空
    if [ -z "${MODEL_DIR_NAMES}" ]; then
        echo "WARNING: MODEL_DIR_NAMES is empty, skipping model processing"
    else
        echo "Processing models: ${MODEL_DIR_NAMES}"
        IFS=',' read -ra MODEL_ARRAY <<< "${MODEL_DIR_NAMES}"
        for model in "${MODEL_ARRAY[@]}"; do
            # Check if model exists
            if [[ ! ${MODEL_SUBDIRS[$model]+_} ]]; then
                echo "Error: Unknown model '${model}'"
                echo "Available models:" "${MODEL_SUBDIRS[@]}"
                continue
            fi

            flag_file="${MODEL_SUBDIRS[$model]}"
            subdir_path="$MODEL_BASE_PATH/$model"
            model_name="${MODEL_NAMES[$model]}"

            echo "Processing model: ${model}"
            echo "model Directory: ${subdir_path}"
            echo "model Name: ${model_name}"

            # Check if subdirectory exists
            if [ ! -d "${subdir_path}" ]; then
                echo "Warning: Subdirectory ${subdir_path} does not exist, creating..."
                mkdir -p "${subdir_path}"
            fi
            # Check if flag file exists
            if [ -f "${subdir_path}/${flag_file}" ]; then
                echo "Flag file ${flag_file} exists in ${subdir_path}, skipping download..."
                # Execute startup script directly
                execute_startup "${subdir_path}" "${model}" "${model_name}"
            else
                echo "Flag file ${flag_file} not found in ${subdir_path}, downloading..."
                # Download and setup first
                download_and_setup "${subdir_path}" "${model_name}" "${flag_file}"
                # Then execute startup script
                execute_startup "${subdir_path}" "${model}" "${model_name}"
            fi
        done
    fi
else
    echo "Non-mounted mode: starting built-in models..."
    for model in tdtsfm timemoe moment; do
        model_dir=$(find_model_file "${model}")
        if [ -n "${model_dir}" ]; then
            echo "✓ Found ${model} model at: ${model_dir}"
            execute_startup "${model_dir}" "${model}" "${MODEL_NAMES[$model]}"
        else
            echo "✗ Model ${model} not found, skipping..."
        fi
    done
fi

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.ini"


if [ ! -f "${CONFIG_FILE}" ]; then
  echo "Error: Configuration file ${CONFIG_FILE} not found!"
  exit 1
fi


echo "Starting uWSGI with config: $CONFIG_FILE"
exec /usr/local/bin/uwsgi --ini "$CONFIG_FILE"

if [ $? -ne 0 ]; then
  echo "uWSGI failed to start. Exiting..."
  exit 1
fi