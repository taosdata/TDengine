#!/bin/bash

# ********************************************************
# Script to uninstall docker-version TAOSDATA on computer
# ********************************************************

headerDir="/usr/local/include/tdengine"
cfgDir="/etc/tdengine"
binDir="/usr/local/bin/tdengine"
libDir="/usr/local/lib/tdengine"
linkDir="/usr/bin"
# 1. Stop continer and remove image
# TODO : Check information
sudo docker container stop tdengined || true
sudo docker container rm tdengined   || true
sudo docker image rm tdengined_img   || true

sudo docker image rm taos_img     || true

# 2. Remove others
## remove binary files
sudo rm -rf {linkDir}/taos {linkDir}/rmtaos ${binDir}

## remove header files
sudo rm -rf ${headerDir}

## remove lib file
sudo rm -rf /usr/lib/libtaos* ${libDir}

## remove configuration file
sudo rm -rf ${cfgDir}

# 3. Remove data
while true; do
    read -p "Do you want to delete data file? [y/N]: " isDeleteData

    if [[ ( "${isDeleteData}" == "y") || ( "${isDeleteData}" == "Y") ]]; then
        sudo docker volume rm -f taos_data taos_log
        break
    elif [[ ( "${isDeleteData}" == "n") || ( "${isDeleteData}" == "N") ]]; then
        break
    else
        continue
    fi
done
