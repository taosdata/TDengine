#!/bin/bash
#
set -e

version=$1
branchName=$2
verType=$3
cpuType=$4
verMode=$5
dockerMode=$6

# docker parameters
password="tbase125!"

scriptDir=$(dirname $(readlink -f $0))
topDir=${scriptDir}/../..         # TDinternal
communityDir=${topDir}/community  # community
echo "make docker for $verMode version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $communityDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug
  cmake ..
fi

if [ "${dockerMode}" == "latest" ]; then
  cd ${communityDir}/packaging/docker
  ./dockerManifest.sh -n ${version} -p ${password} -V ${verType} -a y
  exit
fi

if [ "$verMode" == "cluster" ];then
  tdengineNameType="-enterprise"
  dockerParam=""
elif [ "$verMode" == "cloud" ];then
  cp docker/2.x-files/run.sh ${communityDir}/packaging/docker/
  chmod u+x ${communityDir}/packaging/docker/run.sh
  cp docker/DockerfileCloud.base ${communityDir}/packaging/docker/
  cp docker/DockerfileCloud ${communityDir}/packaging/docker/
  tdengineNameType="-cloud"
  dockerParam="-d y"
else
  tdengineNameType=""
  dockerParam=""
fi

if [ "${dockerMode}" == "push" ]; then
  dockerParam=${dockerParam}" -a y"
fi

if [ "$verType" == "stable" ];then
  verType=stable
  pkgFile=TDengine${tdengineNameType}-server-${version}-Linux-$cpuType.tar.gz
  dockerim=tdengine/tdengine${tdengineNameType}
else
  verType=beta
  pkgFile=TDengine${tdengineNameType}-server-${version}-${verType}-Linux-$cpuType.tar.gz  
  dockerim=tdengine/tdengine${tdengineNameType}-beta
fi

####################### build docker image and push
echo "ready to generate docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ "${cpuType}" == "x64" ] ; then
  cpuType=amd64
  dockerim=${dockerim}-amd64
elif [[ "${cpuType}" == "arm64" ]]; then
  cpuType=aarch64
  dockerim=${dockerim}-aarch64
fi

cd ${communityDir}/packaging/docker
./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${version} -p ${password} -V ${verType} ${dockerParam}

if [ "${dockerMode}" == "push" ] && [ "${verMode}" == "cloud" ]; then
  echo ">>>>>>>>>>>>> check whether the docker image has been published"
  #docker login 49.232.151.239:88
  #docker tag ${dockerim}:$version 49.232.151.239:88/${dockerim}:$version
  #echo "ERROR [generate_docker.sh]: should not be here!"
#  docker push 49.232.151.239:88/${dockerim}:$version
fi
