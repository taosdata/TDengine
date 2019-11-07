#!/bin/bash

# run mnode
docker run --rm -it --name mnode \
    -p 6100:6100 -p 6120:6120 -p 6220:6220 -p 6260:6260 -p 6280:6280 \
    --mount source=taos_data,target=/var/lib/taos/ \
    --mount source=taos_log,target=/var/log/taos/  \
    --mount type=bind,source=/home/hzcheng/Documents/TAOS/taosdata/cfg/,target=/etc/taos/ \
    --network isolated_nw --ip 172.25.0.10 \
    mnode_img
