#!/bin/bash
set -e
#set -x

# dockerbuild.sh 
#             -n [version number]
#             -p [xxxx]
#             -V [stable | beta]

# set parameters by default value
version=""
passWord=""
verType=""

while getopts "hn:p:V:" arg
do
  case $arg in
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
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

echo "version=${version}"

#docker manifest rm tdengine/tdengine
#docker manifest rm tdengine/tdengine:${version}
if [ "$verType" == "beta" ]; then
  docker manifest inspect  tdengine/tdengine-beta:latest
  docker manifest create -a tdengine/tdengine-beta:latest tdengine/tdengine-amd64-beta:latest tdengine/tdengine-aarch64-beta:latest tdengine/tdengine-aarch32-beta:latest
  docker manifest rm tdengine/tdengine-beta:latest
  docker manifest create -a tdengine/tdengine-beta:${version} tdengine/tdengine-amd64-beta:${version} tdengine/tdengine-aarch64-beta:${version} tdengine/tdengine-aarch32-beta:${version}
  docker manifest create -a tdengine/tdengine-beta:latest tdengine/tdengine-amd64-beta:latest tdengine/tdengine-aarch64-beta:latest tdengine/tdengine-aarch32-beta:latest
  docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
  docker manifest push tdengine/tdengine-beta:latest
  docker manifest push tdengine/tdengine-beta:${version}

elif [ "$verType" == "stable" ]; then
  docker manifest inspect  tdengine/tdengine:latest 
  docker manifest create -a tdengine/tdengine:latest tdengine/tdengine-amd64:latest tdengine/tdengine-aarch64:latest tdengine/tdengine-aarch32:latest
  docker manifest rm tdengine/tdengine:latest
  docker manifest create -a tdengine/tdengine:${version} tdengine/tdengine-amd64:${version} tdengine/tdengine-aarch64:${version} tdengine/tdengine-aarch32:${version}
  docker manifest create -a tdengine/tdengine:latest tdengine/tdengine-amd64:latest tdengine/tdengine-aarch64:latest tdengine/tdengine-aarch32:latest
  docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
  docker manifest push tdengine/tdengine:latest
  docker manifest push tdengine/tdengine:${version}

else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

# docker manifest create -a tdengine/${dockername}:${version} tdengine/tdengine-amd64:${version} tdengine/tdengine-aarch64:${version} tdengine/tdengine-aarch32:${version}
# docker manifest create -a tdengine/${dockername}:latest tdengine/tdengine-amd64:latest tdengine/tdengine-aarch64:latest tdengine/tdengine-aarch32:latest

# docker login -u tdengine -p ${passWord}  #replace the docker registry username and password

# docker manifest push tdengine/tdengine:latest

# # how set latest version ???
