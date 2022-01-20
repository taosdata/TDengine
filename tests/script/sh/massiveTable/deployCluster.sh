#!/bin/bash
#
# deploy test cluster

set -e
#set -x

masterDnode=slave
dataRootDir="/data"
firstEp="trd02:7000"
startPort=7000
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

${curr_dir}/cleanCluster.sh -r "/data"
${curr_dir}/cleanCluster.sh -r "/data2"

if [[ "${updateSrc}" == "yes" ]]; then
  ${curr_dir}/compileVersion.sh -r ${curr_dir}/../../../../ -v "3.0"
fi

${curr_dir}/setupDnodes.sh -r "/data"  -n ${dnodeNumber} -f ${firstEp} -p 7000
${curr_dir}/setupDnodes.sh -r "/data2" -n ${dnodeNumber} -f ${firstEp} -p 8000

if [[ "${masterDnode}" == "master" ]]; then
  # create all dnode into cluster
  taos -s "create dnode trd02 port 8000;"
  taos -s "create dnode trd03 port 7000;"
  taos -s "create dnode trd03 port 8000;"
  taos -s "create dnode trd04 port 7000;"
  taos -s "create dnode trd04 port 8000;"
fi




