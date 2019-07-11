#!/bin/bash

# run taos
docker run --rm -it --name taos \
    --mount type=bind,source=/home/hzcheng/.taos_history,target=/root/.taos_history  \
    --mount type=bind,source=/home/hzcheng/Documents/TAOS/taosdata/cfg/,target=/etc/taos/ \
    --network isolated_nw --ip 172.25.0.12 \
    taos_img -p
