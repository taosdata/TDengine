#!/bin/bash

# run dnode
docker run --rm -it --name dnode \
    -p 6140:6140 -p 6160:6160 -p 6180:6180 -p 6200:6200 -p 6240:6240\
    --mount source=taos_data,target=/var/lib/taos/ \
    --mount source=taos_log,target=/var/log/taos/  \
    --mount type=bind,source=/home/hzcheng/Documents/TAOS/taosdata/cfg/,target=/etc/taos/ \
    --network isolated_nw --ip 172.25.0.11 \
    dnode_img
