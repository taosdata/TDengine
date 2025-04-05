#!/bin/bash
#
# setup test environment

set -e
#set -x

# set parameters by default value
dataRootDir="/data/dockerbuilder/storage"
dnodeNumber=1
firstEp="node1:6030"

while getopts "hn:r:f:" arg
do
  case $arg in
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    f)
      firstEp=$(echo $OPTARG)
      ;;
    r)
      dataRootDir=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [ dnode number] "
      echo "                  -n [ dnode number] "
      echo "                  -f [ first ep] "
      echo "                  -r [ data root dir] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "dnodeNumber=${dnodeNumber} dataRootDir=${dataRootDir} firstEp=${firstEp}"

createTaosCfg() {
    cfgFile=$1/cfg/taos.cfg
    firstEp=$2    
    fqdn=$3
    
    echo "debugFlag     131"              > ${cfgFile}
    echo "firstEp       ${firstEp}"      >> ${cfgFile}
    #echo "dataDir       ${dataDir}"     >> ${cfgFile}
    #echo "logDir        ${logDir}"      >> ${cfgFile}
    echo "fqdn          ${fqdn}"         >> ${cfgFile}     
    
    echo "supportVnodes        1024"     >> ${cfgFile} 
    echo "asyncLog             0"        >> ${cfgFile}
    echo "telemetryReporting   0"        >> ${cfgFile}  
}

createDnodesDataDir() {
    if [ -d ${dataRootDir} ]; then
        rm -rf ${dataRootDir}/*
    else
        echo "${dataRootDir} not exist"
        exit 1
    fi
    
    dnodeNumber=$1
    firstEp=$2
    
    serverPort=${startPort}
    for ((i=1; i<=${dnodeNumber}; i++)); do
        mkdir -p ${dataRootDir}/dnode${i}/cfg
        mkdir -p ${dataRootDir}/dnode${i}/log
        mkdir -p ${dataRootDir}/dnode${i}/data
        mkdir -p ${dataRootDir}/dnode${i}/core  
        
        createTaosCfg ${dataRootDir}/dnode${i} ${firstEp} node${i}
    done
}

########################################################################################
###############################  main process ##########################################

## create director and taos.cfg for all dnode
createDnodesDataDir ${dnodeNumber} ${firstEp}

echo "====create end===="
echo " "


