#!/bin/bash

set -e

# new_ver_release.sh  -b [develop | master] 
#                     -c [aarch32 | aarch64 | x64 ...]  
#                     -n [2.1.*.* | 2.0.*.* ]
#                     -l [full | lite] 
#                     -v [cluster, edge ,all] cluster is enterprise, edge is community
#                     -d [isdocker ] 
#                     -h help

# set parameters by default value
branchName=master   # -b [develop | master
cpuType=x64         # -c [aarch32 | aarch64 | x64 ...]
version="2.1.4.1"   # -n [2.1.*.* | 2.0.*.* ]
pagMode=full        # -l [full | lite] 
verMode=all        # -v [cluster, edge ,all ] cluster is enterprise, edge is community
versionComp=2.0.0.0
dockerMode=""

while getopts "hb:c:n:l:v:d:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    b)
      #echo "branchName=$OPTARG"
      branchName=$(echo $OPTARG)
      ;;
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    l)
      #echo "pagMode=$OPTARG"
      pagMode=$(echo $OPTARG)
      ;;
    v)
      #echo "verMode=$OPTARG"
      verMode=$(echo $OPTARG)
      ;;
    d)
      #echo "dockerMode=$OPTARG"
      dockerMode=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master] "
      echo "                          -c [aarch32 | aarch64 | x64 ...] "
      echo "                          -n [version number: 2.1.*.* | 2.0.*.* ]      "
      echo "                          -l [full | lite]  "
      echo "                          -v [cluster, edge ,all] cluster is enterprise, edge is community  "
      echo "                          -d [isdocker ]   "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done


# scripts path
scriptDir=$(dirname $(readlink -f $0))
cd ${scriptDir}
communityDir=${scriptDir}/../../community
comunityArchiveDir=/nas/TDengine/v$version/community   # community version’package directory


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

if [ "$verMode" == "all" ];then
  bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
  bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType
elif [ "$verMode" == "edge" ];then
  bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
elif [ "$verMode" == "cluster" ];then
  bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType
else
  echo "please input right Specified para "
fi

if [ ! -d $comunityArchiveDir ]; then
  mkdir -p $comunityArchiveDir
fi

# docker parameters
dockerPass="tbase125!"
dockerinput_x64=TDengine-server-${version}-beta-Linux-amd64.tar.gz

####################### build docker image and push
if [ "$dockerMode" == "isdocker" ];then
  cd ${scriptDir}
  echo "ready to generate docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
  if [ "${cpuType}" == "x64" ] ; then
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
fi