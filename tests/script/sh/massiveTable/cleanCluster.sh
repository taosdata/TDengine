#!/bin/bash
#
# clean test environment

set -e
#set -x

# cleanCluster.sh  
#          -r [ dnode root dir]


dataRootDir="/data"


while getopts "hr:" arg
do
  case $arg in
    r)
      dataRootDir=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -r [ dnode root dir] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done


rmDnodesDataDir() {
    if [ -d ${dataRootDir} ]; then
        rm -rf ${dataRootDir}/dnode*
    else
        echo "${dataRootDir} not exist"
        exit 1
    fi
}

function kill_process() {
    pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid" ]; then
        kill -9 $pid   || :
    fi
}

########################################################################################
###############################  main process ##########################################

## kill all taosd process
kill_process taosd

rmDnodesDataDir


