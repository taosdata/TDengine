#!/bin/bash

set -e
#
version=$3
versionComp=2.0.0.0

# cpuType= [aarch32 | aarch64 | x64 | x86 | mips64 ...] 
scriptDir=$(dirname $(readlink -f $0))
cd ${scriptDir}
communityDir=${scriptDir}/../../community
comunityArchiveDir=/nas/TDengine/v$version/community   # community version’package directory
branchName=$1
cpuType=$2
dockerPass="tbase125!"
dockerinput_x64=TDengine-server-${version}-Linux-amd64.tar.gz

if [ "$branchName" == "master" ];then
  branchName=master
  verType=stable
  tagVal=ver-${version}
  dockerinput=TDengine-server-${version}-Linux-$cpuType.tar.gz
  dockerim=tdengine/tdengine
elif [ "$branchName" == "develop" ];then
  branchName=develop
  verType=beta
  tagVal=ver-${version}-beta
  dockerinput=TDengine-server-${version}-${verType}-Linux-$cpuType.tar.gz  
  dockerim=tdengine/tdengine-beta
fi


bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType

cd ${scriptDir}
####################### build docker image and push
echo "ready to generate docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [[ "${cpuType}" == "x64" ]] ; then
  cpuType=amd64
  # cd ${communityDir}/packaging/docker
  cp -f ${comunityArchiveDir}/${dockerinput}  ${comunityArchiveDir}/${dockerinput_x64}
  bash generate_docker.sh     $version $dockerPass  $branchName $verType $cpuType ${dockerinput_x64}
  # echo ">>>>>>>>>>>>> check whether the docker image has been published"
  # docker pull ${dockerim}:${version}
  # docker tag ${dockerim}:$version ${dockerim}:latest
  # docker push tdengine/tdengine:latest
elif [[ "${cpuType}" == "aarch64" ]] || [[ "${cpuType}" == "aarch32" ]]; then
  # cd $communityDir/packaging/docker
  echo `pwd`
  bash generate_docker.sh    $version $dockerPass  $branchName $verType $cpuType ${dockerinput}

  # echo ">>>>>>>>>>>>> check whether the docker image has been published"
  # docker pull ${dockerim}-${cpuType}:${version}
  # docker tag ${dockerim}-${cpuType}:$version ${dockerim}-${cpuType}:latest
  # docker push tdengine/tdengine-aarch64:latest
fi