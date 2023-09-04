#!/bin/bash
#
set -e
# set -x

version=$1
branchName=$2
verType=$3
cpuType=$4
verMode=$5
dockerMode=$6
dockerProject=$7

# docker parameters
password="tBase125!"

scriptDir=$(dirname $(readlink -f $0))
topDir=${scriptDir}/../..         # TDinternal
enterpriseDir=${topDir}/enterprise # enterprise
echo "make docker for $verMode version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $enterpriseDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug
  cmake ..
fi

if [ "${dockerMode}" == "latest" ]; then
  cd ${enterpriseDir}/packaging/docker
  ./dockerManifest-enterprise.sh -n ${version} -p ${password} -V ${verType} -a y -D ${dockerProject}
  exit
fi

if [ "$verMode" == "cluster" ];then
  tdengineNameType="-enterprise"
  dockerParam=""
elif [ "$verMode" == "cloud" ];then
  chmod u+x ${enterpriseDir}/packaging/docker/run.sh
  chmod u+x ${enterpriseDir}/packaging/docker/td_cluster_check
  cp docker/run.sh ${enterpriseDir}/packaging/docker/
  cp docker/td_cluster_check ${enterpriseDir}/packaging/docker/
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
  dockerim=${dockerProject}/tdengine${tdengineNameType}
else
  verType=beta
  pkgFile=TDengine${tdengineNameType}-server-${version}-${verType}-Linux-$cpuType.tar.gz  
  dockerim=${dockerProject}/tdengine${tdengineNameType}-beta
fi

####################### build docker image and push
echo "ready to generate docker for enterprise version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ "${cpuType}" == "x64" ] ; then
  cpuType=amd64
  dockerim=${dockerim}-amd64
elif [[ "${cpuType}" == "arm64" ]]; then
  cpuType=aarch64
  dockerim=${dockerim}-aarch64
fi

cd ${enterpriseDir}/packaging/docker
./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${version} -p ${password} -V ${verType} -D ${dockerProject} ${dockerParam}

if [ "${dockerMode}" == "push" ] && [ "${verMode}" == "cloud" ]; then
  echo ">>>>>>>>>>>>> check whether the docker image has been published"
  docker login image.cloud.taosdata.com
  docker tag ${dockerim}:$version image.cloud.taosdata.com/${dockerim}:$version
#  docker push 49.232.151.239:88/${dockerim}:$version
fi
