#!/bin/bash

csudo=""
if command -v sudo >/dev/null; then
  csudo="sudo "
fi

prefix="taos"

services=(${prefix}"d" ${prefix}"adapter" ${prefix}"x" ${prefix}"-explorer" ${prefix}"keeper")


for service in "${services[@]}"; do    
    ${csudo}systemctl start $service
    if systemctl is-active $service >/dev/null; then
        echo "$service has been started successfully"
    else
        echo "failed to start $service"
    fi
done