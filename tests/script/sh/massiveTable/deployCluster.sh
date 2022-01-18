#!/bin/bash
#
# deploy test cluster

set -e
#set -x

# deployCluster.sh 

curr_dir=$(pwd)
echo "currect pwd: ${curr_dir}"

./cleanCluster.sh -r "/data"
./cleanCluster.sh -r "/data2"

./compileVersion.sh -r ${curr_dir}/../../../../ -v "3.0"

./setupDnodes.sh -r "/data"  -n 1 -f "trd02:7000" -p 7000
./setupDnodes.sh -r "/data2" -n 1 -f "trd02:7000" -p 8000

#./setupDnodes.sh -r "/data"  -n 2 -f trd02:7000 -p 7000
#./setupDnodes.sh -r "/data2" -n 2 -f trd02:7000 -p 8000




