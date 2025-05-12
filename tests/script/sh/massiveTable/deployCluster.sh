#!/bin/bash
#
# deploy test cluster

set -e
#set -x

fqdn=`hostname`

masterDnode=slave
dataRootDir="/data"
firstEp="${fqdn}:6030"
startPort=6030
dnodeNumber=1
updateSrc=no

while getopts "hm:f:n:r:p:u:" arg
do
  case $arg in
    m)
      masterDnode=$( echo $OPTARG )
      ;;
    n)
      dnodeNumber=$(echo $OPTARG)
      ;;
    u)
      updateSrc=$(echo $OPTARG)
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
      echo "Usage: `basename $0` -m [if master dnode] "
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

# deployCluster.sh 
curr_dir=$(readlink -f "$(dirname "$0")")
echo $curr_dir

${curr_dir}/cleanCluster.sh -r ${dataRootDir}

if [[ "${updateSrc}" == "yes" ]]; then
  ${curr_dir}/compileVersion.sh -r ${curr_dir}/../../../../ -v "3.0"
fi

${curr_dir}/setupDnodes.sh -r ${dataRootDir}  -n ${dnodeNumber} -f ${firstEp} -p ${startPort}

if [[ "${masterDnode}" == "master" ]]; then
  taos -s "create dnode trd03 port 6030;"
  taos -s "create dnode trd04 port 6030;"
fi




