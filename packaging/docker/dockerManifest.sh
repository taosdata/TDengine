#!/bin/bash
set -e
#set -x

# dockerbuild.sh 
#             -n [version number]
#             -p [xxxx]

# set parameters by default value
verNumber=""
passWord=""

while getopts "hn:p:" arg
do
  case $arg in
    n)
      #echo "verNumber=$OPTARG"
      verNumber=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [version number] "
      echo "                      -p [password for docker hub] "
      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "verNumber=${verNumber}"

#docker manifest create -a tdengine/tdengine:${verNumber} tdengine/tdengine-amd64:${verNumber} tdengine/tdengine-aarch64:${verNumber} tdengine/tdengine-aarch32:${verNumber}
docker manifest create -a tdengine/tdengine tdengine/tdengine-amd64:latest tdengine/tdengine-aarch64:latest tdengine/tdengine-aarch32:latest

docker login -u tdengine -p ${passWord}  #replace the docker registry username and password

docker manifest push tdengine/tdengine

# how set latest version ???
