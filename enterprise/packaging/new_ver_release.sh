#!/bin/bash

set -e
# set -x

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
dockerProject="tdengine"
grantValue=60

while getopts "hb:c:n:l:v:d:V:N:P:M:D:G:" arg
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
    N)
      #echo "cusName=$OPTARG"
      cusName=$(echo $OPTARG)
      ;;
    P)
      #echo "cusPrompt=$OPTARG"
      cusPrompt=$(echo $OPTARG)
      ;;
    M)
      #echo "cusEmail=$OPTARG"
      cusEmail=$(echo $OPTARG)
      ;;
    D)
      #echo "cusEmail=$OPTARG"
      dockerProject=$(echo $OPTARG)
      ;;
    G)
      grantValue=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master] "
      echo "                     -c [aarch32 | aarch64 | x64 ...] "
      echo "                     -n [version number: 2.1.*.* | 2.0.*.* ]      "
      echo "                     -l [full | lite]  "
      echo "                     -v [cluster, edge ,all] cluster is enterprise, edge is community  "
      echo "                     -V [stable | beta] "
      echo "                     -d [no | build | push | latest]   "
      echo "                     -N <custom name>"
      echo "                     -P <custom prompt>"
      echo "                     -M <custom email>"
      echo "                     -D <harbor docker project>"
      echo "                     -G <grant days>"
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

scriptDir=$(dirname $(realpath $0 || readlink -f $0))
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
enterpriseDir=$topDir/enterprise

if [ "$dockerMode" == "latest" ];then
  if [ "$verMode" == "cluster" ];then
    bash generate_docker_enterprise.sh     $version $branchName $verType $cpuType $verMode $dockerMode $dockerProject
  else
    bash generate_docker.sh     $version $branchName $verType $cpuType $verMode $dockerMode
  fi
fi

if [ "$verMode" == "all" ];then
  bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
  bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType $grantValue $cusName $cusPrompt $cusEmail
elif [ "$verMode" == "edge" ];then
  bash generate_community.sh  $version $versionComp $branchName $verType $cpuType
elif [ "$verMode" == "cluster" ];then
  echo  "bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType $grantValue $cusName $cusPrompt $cusEmail"
  bash generate_enterprise.sh $version $versionComp $branchName $verType $cpuType $grantValue $cusName $cusPrompt $cusEmail
elif [ "$verMode" == "cloud" ];then
  bash generate_cloud.sh $version $versionComp $branchName $verType $cpuType
else
  echo "please input right Specified para "
fi

if [ "$dockerMode" == "build" ] || [ "$dockerMode" == "push" ];then
  if [ "$verMode" == "cluster" ];then
    cp -f $communityDir/release/TDengine-enterprise-server-*-Linux-x64.tar.gz $enterpriseDir/packaging/docker
    bash generate_docker_enterprise.sh     $version $branchName $verType $cpuType $verMode $dockerMode $dockerProject
  else
    bash generate_docker.sh     $version $branchName $verType $cpuType $verMode $dockerMode
  fi
fi

if [[ ! -z "${cusName}" || ! -z "${cusPrompt}" || ! -z "${cusEmail}" ]];then
    echo "custom name: ${cusName}, custom prompt: ${cusPrompt}, custom email: ${cusEmail}"
    echo "communityDir: ${communityDir}, enterpriseDir: ${enterpriseDir}"
    python3 ./repack-release.py -n ${cusName} -p ${cusPrompt} -e ${cusEmail} -d ../../community/release -v ${version}
fi
