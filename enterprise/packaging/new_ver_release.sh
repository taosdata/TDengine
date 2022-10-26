#!/bin/bash

set -e

# new_ver_release.sh  -b [develop | master] 
#                     -c [aarch32 | aarch64 | x64 ...]  
#                     -n [2.1.*.* | 2.0.*.* ]
#                     -l [full | lite] 
#                     -v [cluster, edge ,all] cluster is enterprise, edge is community
#                     -V [stable | beta]
#                     -d [isdocker ] 
#                     -h help

# set parameters by default value
branchName=master   # -b [develop | master ]
cpuType=x64         # -c [aarch32 | aarch64 | x64 ...]
version="3.0.0.0"   # -n [2.1.*.* | 2.0.*.* ]
pagMode=full        # -l [full | lite] 
verMode=all         # -v [cluster, edge ,all ] cluster is enterprise, edge is community
verType=stable      # -V [stable, beta]
versionComp=3.0.0.0
dockerMode="no"

while getopts "hb:c:n:l:v:d:V:" arg
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
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
      ;;
    d)
      #echo "dockerMode=$OPTARG"
      dockerMode=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master] "
      echo "                     -c [aarch32 | aarch64 | x64 ...] "
      echo "                     -n [version number: 2.1.*.* | 2.0.*.* ]      "
      echo "                     -l [full | lite]  "
      echo "                     -v [cluster, edge ,all] cluster is enterprise, edge is community  "
      echo "                     -V [stable | beta] "
      echo "                     -d [no | build | push]   "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

if [ "$dockerMode" == "latest" ];then
  bash generate_docker.sh     $version $branchName $verType $cpuType $verMode $dockerMode
fi

# if [ "$verMode" == "all" ];then
#   bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
#   bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType
# elif [ "$verMode" == "edge" ];then
#   bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
# elif [ "$verMode" == "cluster" ];then
#   bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType
# elif [ "$verMode" == "cloud" ];then
#   bash generate_cloud.sh $version $versionComp $branchName $verType $cpuType
# else
#   echo "please input right Specified para "
# fi

if [ "$dockerMode" == "build" ] || [ "$dockerMode" == "push" ];then
  bash generate_docker.sh     $version $branchName $verType $cpuType $verMode $dockerMode
fi