#!/bin/bash

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

prefix="taos"

services=(${prefix}"d" ${prefix}"adapter" ${prefix}"x" ${prefix}"-explorer" ${prefix}"keeper")


for service in "${services[@]}"; do    
    ${csudo}systemctl stop $service
    if systemctl is-active $service >/dev/null; then
        echo "$service failed to stop"
    else        
        echo "$service has stoped"
    fi
done