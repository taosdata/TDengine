#!/bin/bash
#
# setup test environment

set -e
#set -x

# setupDnodes.sh  
#             -e [ new | old]
#             -n [ dnode number]
#             -f [ first ep]
#             -p [ start port]
#             -r [ dnode root dir]

# set parameters by default value
fqdn=`hostname`

enviMode=new
dataRootDir="/data"
firstEp="${fqdn}:6030"
startPort=6030
dnodeNumber=1


while getopts "he:f:n:r:p:" arg
do
  case $arg in
    e)
      enviMode=$( echo $OPTARG )
      ;;
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    f)
      firstEp=$(echo $OPTARG)
      ;;
    p)
      startPort=$(echo $OPTARG)
      ;;
    r)
      dataRootDir=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -e [new | old] "
      echo "                  -n [ dnode number] "
      echo "                  -f [ first ep] "
      echo "                  -p [ start port] "
      echo "                  -r [ dnode root dir] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "enviMode=${enviMode} dnodeNumber=${dnodeNumber} dataRootDir=${dataRootDir} firstEp=${firstEp} startPort=${startPort}"

#curr_dir=$(pwd)


createNewCfgFile() {
    cfgFile=$1/taos.cfg
    dataDir=$2
    logDir=$3
    firstEp=$4    
    serverPort=$5
    
    echo "debugFlag     131"             > ${cfgFile}
    echo "firstEp       ${firstEp}"     >> ${cfgFile}
    echo "dataDir       ${dataDir}"     >> ${cfgFile}
    echo "logDir        ${logDir}"      >> ${cfgFile}
    echo "serverPort    ${serverPort}"  >> ${cfgFile}     
    echo "numOfLogLines      100000000" >> ${cfgFile}  
    echo "supportVnodes        1024"   >> ${cfgFile} 
    #echo "asyncLog             0"     >> ${cfgFile}
    echo "telemetryReporting   0"      >> ${cfgFile}  
}

createNewDnodesDataDir() {
    if [ -d ${dataRootDir} ]; then
        rm -rf ${dataRootDir}/dnode*
    else
        echo "${dataRootDir} not exist"
        exit 1
    fi
    
    dnodeNumber=$1
    firstEp=$2
    
    serverPort=${startPort}
    for ((i=0; i<${dnodeNumber}; i++)); do
        mkdir -p ${dataRootDir}/dnode_${i}/cfg
        mkdir -p ${dataRootDir}/dnode_${i}/log
        mkdir -p ${dataRootDir}/dnode_${i}/data 
        
        createNewCfgFile ${dataRootDir}/dnode_${i}/cfg ${dataRootDir}/dnode_${i}/data ${dataRootDir}/dnode_${i}/log ${firstEp} ${serverPort}
        #echo "create dnode: ${serverPort}, ${dataRootDir}/dnode_${i}"
        serverPort=$((10#${serverPort}+100))
    done
}

function kill_process() {
    pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid" ]; then
        kill -9 $pid   || :
    fi
}

startDnodes() {
    dnodeNumber=$1
    
    for ((i=0; i<${dnodeNumber}; i++)); do
        if [ -d ${dataRootDir}/dnode_${i} ]; then
            nohup taosd -c ${dataRootDir}/dnode_${i}/cfg >/dev/null 2>&1 & 
            echo "start taosd ${dataRootDir}/dnode_${i}"
        fi
    done
}

########################################################################################
###############################  main process ##########################################

## kill all taosd process
#kill_process taosd

## create director for all dnode
if [[ "$enviMode" == "new" ]]; then
  createNewDnodesDataDir ${dnodeNumber} ${firstEp}  
fi

## start all dnode by nohup
startDnodes ${dnodeNumber}

echo "====run setupDnodes.sh end===="
echo " "


