#!/bin/bash

set -e
#
version=2.1.4.1
versionComp=2.0.0.0

# cpuType= [aarch32 | aarch64 | x64 | x86 | mips64 ...] 
scriptdir=$(dirname $(readlink -f $0))
cd ${scriptdir}
communityDir=${scriptdir}/../../community
comunity_archiveDir=/nas/TDengine/v$version/community   # community version’package directory
branchName=$1
cpuType=$2
dockerPass="tbase125!"
dockerinput_x64=TDengine-server-${version}-Linux-amd64.tar.gz

if [ "$branchName" == "master" ];then
  branchName=master
  verType=stable
  tagVal=ver-${version}
  pkgFile=TDengine-server-${version}-Linux-x64.tar.gz
  dockerinput=TDengine-server-${version}-Linux-$cpuType.tar.gz
  dockerim=tdengine/tdengine
elif [ "$branchName" == "develop" ];then
  branchName=develop
  verType=beta
  tagVal=ver-${version}-beta
  pkgFile=TDengine-server-${version}-Linux-x64-beta.tar.gz
  dockerinput=TDengine-server-${version}-Linux-$cpuType-beta.tar.gz  
  dockerim=tdengine/tdengine-beta
fi


bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType


# ####################### build docker image and push
# echo "make docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# if [[ "${cpuType}" == "x64" ]] ; then
#   cpuType=amd64
#   cd ${communityDir}/packaging/docker
#   cp -f ${comunity_archiveDir}/${pkgFile} ../../release/${dockerinput_x64}
#    ./dockerbuildi.sh -c ${cpuType} -n ${version}  -p ${dockerPass}
#   echo ">>>>>>>>>>>>> check whether the docker image has been published"
#   docker pull ${dockerim}:${version}
#   docker tag ${dockerim}:$version ${dockerim}:latest
#   docker push tdengine/tdengine:latest
# elif [[ "${cpuType}" == "aarch64" ]] || [[ "${cpuType}" == "aarch32" ]]; then
#   cd $communityDir/packaging/docker
#   cp -f ${comunity_archiveDir}/${pkgFile} ../../release/${dockerinput}
#    ./dockerbuildi.sh -c ${$cpuType} -n ${version}  -p ${dockerPass}
#   echo ">>>>>>>>>>>>> check whether the docker image has been published"
#   docker pull ${dockerim}-${cpuType}:${version}
#   docker tag ${dockerim}-${cpuType}:$version ${dockerim}-${cpuType}:latest
#   docker push tdengine/tdengine-aarch64:latest
# fi