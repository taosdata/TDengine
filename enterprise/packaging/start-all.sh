#!/bin/bash

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
        if [ "$EUID" -eq 0 ]; then
            sysctl_cmd_arr=(systemctl)
        else
            sysctl_cmd_arr=(systemctl --user)
        fi
        "${sysctl_cmd_arr[@]}" start "$1"
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if "${sysctl_cmd_arr[@]}" is-active "$1" >/dev/null; then
                echo "$1 has been started successfully"
                break            
            fi
            MAX_RETRY=$((MAX_RETRY - 1))
        done
        if [ $MAX_RETRY -eq 0 ]; then
            echo "failed to start $1"
        fi
    elif [ "$osType" == "Darwin" ]; then
        # macOS launchctl: user-level
        if [ "$EUID" -eq 0 ]; then
            domain="system"
        else
            domain="gui/$(id -u)"
        fi
        launchctl start "com.tdengine.$1"
        while [ $MAX_RETRY -gt 0 ]; do
            sleep 0.5
            if launchctl print "${domain}/com.tdengine.$1" 2>/dev/null | grep 'state = running' > /dev/null; then
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
cfg_dir="/etc/taos"
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

if [ -x "${install_main_dir}/bin/create_snode.sh" ]; then
  CFG_DIR=${cfg_dir} "${install_main_dir}/bin/create_snode.sh"
fi