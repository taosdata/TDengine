#!/bin/bash
set -e
#set -x
set -v

# dockerbuild.sh
#             -n [version number]
#             -p [xxxx]
#             -V [stable | beta]

# set parameters by default value
version=""
passWord=""
verType=""
dockerLatest="n"

while getopts "hn:p:V:a:D:" arg
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
      echo "                     -p [password for docker hub] "
      echo "                     -V [stable |beta] "
      echo "                     -a [y | n ]   "
      exit 0
      ;;
    a)
      #echo "dockerLatest=$OPTARG"
      dockerLatest=$(echo $OPTARG)
      ;;
    D)
      #echo "dockerLatest=$OPTARG"
      dockerProject=$(echo $OPTARG)
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "version=${version}"

if [ "$verType" == "stable" ]; then
  verType=stable
  dockerinput=TDengine-enterprise-server-${version}-Linux-$cpuType.tar.gz
  dockerinput_x64=TDengine-enterprise-server-${version}-Linux-amd64.tar.gz
  dockerim=${dockerProject}/tdengine-enterprise
  dockeramd64=${dockerProject}/tdengine-enterprise-amd64
  dockeraarch64=${dockerProject}/tdengine-enterprise-aarch64
  dockeraarch32=${dockerProject}/tdengine-enterprise-aarch32
elif [ "$verType" == "beta" ];then
  verType=beta
  tagVal=ver-${version}-beta
  dockerinput=TDengine-enterprise-server-${version}-${verType}-Linux-$cpuType.tar.gz
  dockerinput_x64=TDengine-enterprise-server-${version}-${verType}-Linux-amd64.tar.gz
  dockerim=${dockerProject}/tdengine-enterprise-beta
  dockeramd64=${dockerProject}/tdengine-enterprise-amd64-beta
  dockeraarch64=${dockerProject}/tdengine-enterprise-aarch64-beta
  dockeraarch32=${dockerProject}/tdengine-enterprise-aarch32-beta
 else
  echo "unknown verType, nor stabel or beta"
  exit 1
fi

username="internaltest"

# generate docker version
echo "generate ${dockerim}:${version}"
docker login https://image.cloud.taosdata.com -u ${username} -p ${passWord}
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker manifest rm ${dockerim}:${version}
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker manifest push ${dockerim}:${version}


# generate docker latest
echo "generate ${dockerim}:latest "

if  [ ${dockerLatest} == 'y' ]  ;then
    echo "docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest"
    docker login https://image.cloud.taosdata.com -u ${username} -p ${passWord}
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker manifest rm ${dockerim}:latest
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker manifest push ${dockerim}:latest
    docker pull ${dockerProject}/tdengine-enterprise:latest

fi

