#!/bin/bash
#
# deploy test cluster

set -e
#set -x

# deployCluster.sh 

curr_dir=$(pwd)

source ./cleanCluster.sh -r /data
source ./cleanCluster.sh -r /data2

source ./compileVersion.sh -r ${curr_dir}/../../../../ -v "3.0"

source ./setupDnodes.sh -r /data  -n 1 -f trd02:7000 -p 7000
source ./setupDnodes.sh -r /data2 -n 1 -f trd02:7000 -p 8000

#source ./setupDnodes.sh -r /data  -n 2 -f trd02:7000 -p 7000
#source ./setupDnodes.sh -r /data2 -n 2 -f trd02:7000 -p 8000




