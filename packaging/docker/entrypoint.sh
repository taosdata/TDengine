#!/bin/bash

export PATH="/usr/local/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# Define directories and their associated files/scripts
declare -A DIR_CHECKS=(
    ["/usr/local/taos/taosanode/cfg"]="taosanode.ini"
    ["/usr/local/taos/taosanode/data"]="data_initialized.flag"
    ["/usr/local/taos/taosanode/models"]="model_initialized.flag"
    ["/usr/local/taos/taosanode/scripts"]="download_and_setup.sh"
    ["/usr/local/taos/taosanode/logs"]="logs_initialized.flag"
)

# Check each directory and handle missing files
for dir in "${!DIR_CHECKS[@]}"; do
    file="${DIR_CHECKS[$dir]}"

    # Check if directory is mounted
    if mount | grep -q "$dir"; then
        echo "Directory $dir is mounted"

        # Check if required file exists
        if [ ! -f "$dir/$file" ]; then
            echo "File $file not found in $dir, attempting to download..."

            # Activate virtual environment
            source /usr/local/taos/taosanode/venv/bin/activate

            # Run download script (adjust script name/path as needed)
            if [ -f "/usr/local/taos/taosanode/scripts/download_${file%.*}.sh" ]; then
                "/usr/local/taos/taosanode/scripts/download_${file%.*}.sh" "$dir"
            else
                echo "Warning: Download script for $file not found"
            fi

            # Verify file was downloaded
            if [ ! -f "$dir/$file" ]; then
                echo "Error: Failed to download required file $file in $dir"
                exit 1
            fi

            # Start associated service if available
            if [ -f "/usr/local/taos/taosanode/scripts/start-${file%.*}.sh" ]; then
                echo "Starting service for $file..."
                "/usr/local/taos/taosanode/scripts/start-${file%.*}.sh" "$dir" &
            fi
        else
            echo "File $file already exists in $dir"
        fi
    else
        echo "Directory $dir not mounted, skipping..."
    fi
done

export PATH="/usr/local/taos/taosanode/venv/bin:$PATH"
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

CONFIG_FILE="/usr/local/taos/taosanode/cfg/taosanode.ini"
TS_SERVER_FILE="/root/taos_ts_server.py"
TIME_MOE_FILE="/root/time-moe/time-moe_server.py"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file $CONFIG_FILE not found!"
  exit 1
fi


if [ -f $TS_SERVER_FILE ];then
  echo "Starting tdtsfm server..."
  python3 $TS_SERVER_FILE --action server &
  TAOS_TS_PID=$!

  if ! ps -p $TAOS_TS_PID > /dev/null; then
    echo "Error: tdtsfm server failed to start!"
    exit 1
  fi
fi
if [ -f $TIME_MOE_FILE ];then
  echo "Starting time-moe server..."
  cd $(dirname "$TIME_MOE_FILE")
  python3 $TIME_MOE_FILE --action server &
  TIME_MOE_PID=$!

  if ! ps -p $TIME_MOE_PID > /dev/null; then
    echo "Error: time-moe server failed to start!"
    exit 1
  fi
fi

echo "Starting uWSGI with config: $CONFIG_FILE"
exec /usr/local/taos/taosanode/venv/bin/uwsgi --ini "$CONFIG_FILE"

if [ $? -ne 0 ]; then
  echo "uWSGI failed to start. Exiting..."
  exit 1
fi
