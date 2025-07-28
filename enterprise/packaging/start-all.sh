#!/bin/bash

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

prefix="taos"
versionType="enterprise"
mode="full"
osType=$(uname)
version="3.3.7.0"
MAX_RETRY=3

if [ "${versionType}" == "enterprise" ] && [ "${mode}" == "full" ]; then
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"x" ${prefix}"-explorer" ${prefix}"keeper")
else
    services=(${prefix}"d" ${prefix}"adapter" ${prefix}"-explorer" ${prefix}"keeper")
fi

function start_service() {
    if [ "$osType" == "Linux" ]; then
        ${csudo}systemctl start $1
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if systemctl is-active $1 >/dev/null; then
                echo "$1 has been started successfully"
                break            
            fi
            MAX_RETRY=$((MAX_RETRY - 1))
        done
        if [ $MAX_RETRY -eq 0 ]; then
            echo "failed to start $1"
        fi
    elif [ "$osType" == "Darwin" ]; then
        ${csudo}launchctl start com.tdengine.$1
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if launchctl print system/com.tdengine.$1 | grep 'state = running' > /dev/null; then
                echo "$1 has been started successfully"
                break
            fi
            MAX_RETRY=$((MAX_RETRY - 1))
        done
        if [ $MAX_RETRY -eq 0 ]; then
            echo "failed to start $1"
        fi
    fi
}

for service in "${services[@]}"; do
    start_service $service
done

sleep 5

if [ "$osType" != "Darwin" ]; then
  install_main_dir="/usr/local/taos"
else
  if [ -d "/usr/local/Cellar/" ];then
    install_main_dir="/usr/local/Cellar/tdengine/${version}"
  elif [ -d "/opt/homebrew/Cellar/" ];then
    install_main_dir="/opt/homebrew/Cellar/tdengine/${version}"
  else
    install_main_dir="/usr/local/taos"
  fi
fi

    
if [ -x ${install_main_dir}/bin/create_snode.sh ]; then
  ${csudo} ${install_main_dir}/bin/create_snode.sh
else
  echo "create_snode.sh not found or not executable"
fi