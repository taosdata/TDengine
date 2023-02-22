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

while getopts "hn:p:V:a:" arg
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
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "version=${version}"

if [ "$verType" == "stable" ]; then
  verType=stable
  dockerinput=TDengine-server-${version}-Linux-$cpuType.tar.gz
  dockerinput_x64=TDengine-server-${version}-Linux-amd64.tar.gz
  dockerim=tdengine/tdengine
  dockeramd64=tdengine/tdengine-amd64
  dockeraarch64=tdengine/tdengine-aarch64
  dockeraarch32=tdengine/tdengine-aarch32
elif [ "$verType" == "beta" ];then
  verType=beta
  tagVal=ver-${version}-beta
  dockerinput=TDengine-server-${version}-${verType}-Linux-$cpuType.tar.gz
  dockerinput_x64=TDengine-server-${version}-${verType}-Linux-amd64.tar.gz
  dockerim=tdengine/tdengine-beta
  dockeramd64=tdengine/tdengine-amd64-beta
  dockeraarch64=tdengine/tdengine-aarch64-beta
  dockeraarch32=tdengine/tdengine-aarch32-beta
 else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

username="tdengine"

# generate docker version
echo "generate ${dockerim}:${version}"
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker manifest rm ${dockerim}:${version}
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker login -u ${username} -p ${passWord}
docker manifest push ${dockerim}:${version}


# generate docker latest
echo "generate ${dockerim}:latest "

if  [ ${dockerLatest} == 'y' ]  ;then
    echo "docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest"
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker manifest rm ${dockerim}:latest
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
    docker manifest push ${dockerim}:latest
    docker pull tdengine/tdengine:latest

fi


