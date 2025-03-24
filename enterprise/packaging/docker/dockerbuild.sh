#!/bin/bash
#

set -e
# set -x

# dockerbuild.sh
#             -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...]
#             -n [version number]
#             -p [password for docker hub]
#             -V [stable | beta]
#             -f [pkg file]

# set parameters by default value
cpuType=""
cpuTypeAlias=""
version=""
passWord=""
pkgFile=""
tdgptPkgFile=""
verType="stable"
dockerLatest="n"
cloudBuild="n"
dockerProject="tdengine"
nasIp="0.0.0.0"

while getopts "hc:n:p:f:V:g:i:a:b:d:D:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    f)
      #echo "pkgFile=$OPTARG"
      pkgFile=$(echo $OPTARG)
      ;;
    b)
      #echo "branchName=$OPTARG"
      branchName=$(echo $OPTARG)
      ;;
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
      ;;
    g)
      #echo "tdgptPkgFile=$OPTARG"
      tdgptPkgFile=$(echo $OPTARG)
      ;;
    i)
      #echo "nasIp=$OPTARG"
      nasIp=$(echo $OPTARG)
      ;;
    d)
      #echo "cloudBuild=$OPTARG"
      cloudBuild=$(echo $OPTARG)
      ;;
    a)
      #echo "dockerLatest=$OPTARG"
      dockerLatest=$(echo $OPTARG)
      ;;
    D)
      #echo "dockerLatest=$OPTARG"
      dockerProject=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0`  -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...] "
      echo "                      -n [version number] "
      echo "                      -p [password for docker hub] "
      echo "                      -V [stable | beta] "
      echo "                      -g [pkg name for anode] "
      echo "                      -i [nasIp] "
      echo "                      -f [pkg file] "
      echo "                      -a [y | n ]   "
      echo "                      -d [cloud build ] "
      echo "                      -D [harbor docker project] "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done


# Check_version()
# {
# }


if [ "$verType" == "beta" ]; then
  dockername=${cpuType}-${verType}
  dirName=${pkgFile%-beta*}
  if [ -n "$tdgptPkgFile" ];then
    tdgptDirName=${tdgptPkgFile%-beta*}
  fi
elif [ "$verType" == "stable" ]; then
  dockername=${cpuType}
  dirName=${pkgFile%-Linux*}
  if [ -n "$tdgptPkgFile" ];then
    tdgptDirName=${tdgptPkgFile%-Linux*}
  fi
else
  echo "unknown verType, nor stabel or beta"
  exit 1
fi
if [ "$cloudBuild" == "y" ]; then
  dockername=cloud-${dockername}
fi


echo "cpuType=${cpuType} version=${version} pkgFile=${pkgFile} verType=${verType} "
echo "$(pwd)"
echo "====NOTES: ${pkgFile} must be in the same directory as dockerbuild.sh===="

scriptDir=$(dirname $(readlink -f $0))
enterpriseDir=${scriptDir}/../../../enterprise
DockerfilePath=${enterpriseDir}/packaging/docker/
if [ "$cloudBuild" == "y" ]; then
  communityArchiveDir=/nas/TDengine/v$version/cloud
  if [ -n "$tdgptPkgFile" ];then
    Dockerfile=${enterpriseDir}/packaging/docker/DockerfileCloudTDgpt
  else
    Dockerfile=${enterpriseDir}/packaging/docker/DockerfileCloud
  fi
else
  communityArchiveDir=/nas/TDengine/v$version/community
  Dockerfile=${enterpriseDir}/packaging/docker/Dockerfile
fi
cd ${scriptDir}
cp -f ${communityArchiveDir}/${pkgFile} . || echo "failed to copy file from ${communityArchiveDir}"

echo "dirName=${dirName}"

if [[ "${cpuType}" == "x64" ]] || [[ "${cpuType}" == "amd64" ]]; then
    cpuTypeAlias="amd64"
elif [[ "${cpuType}" == "aarch64" ]]; then
    cpuTypeAlias="arm64"
elif [[ "${cpuType}" == "aarch32" ]]; then
    cpuTypeAlias="armhf"
else
    echo "Unknown cpuType: ${cpuType}"
    exit 1
fi

if [ -n "$tdgptPkgFile" ];then
  docker build --rm -f "${Dockerfile}"  --network=host -t ${dockerProject}/tdengine-enterprise-${dockername}:${version} "." --build-arg pkgFile=${pkgFile}  --build-arg dirName=${dirName} --build-arg tdgptPkgFile=${tdgptPkgFile}  --build-arg tdgptDirName=${tdgptDirName}  --build-arg cpuType=${cpuTypeAlias} --build-arg nasIp=${nasIp}
else
  docker build --rm -f "${Dockerfile}"  --network=host -t ${dockerProject}/tdengine-enterprise-${dockername}:${version} "." --build-arg pkgFile=${pkgFile} --build-arg dirName=${dirName} --build-arg cpuType=${cpuTypeAlias}
fi

docker logout
docker login https://image.cloud.taosdata.com -u internaltest -p ${passWord}  #replace the docker registry username and password

if [ "$cloudBuild" != "y" ]; then
  docker tag ${dockerProject}/tdengine-enterprise-${dockername}:${version} image.cloud.taosdata.com/${dockerProject}/tdengine-enterprise-${dockername}:${version}
  docker push image.cloud.taosdata.com/${dockerProject}/tdengine-enterprise-${dockername}:${version}
fi

# set this version to latest version
if  [ "$cloudBuild" != "y" ] && [ ${dockerLatest} == 'y' ]  ;then
  docker tag ${dockerProject}/tdengine-enterprise-${dockername}:${version} image.cloud.taosdata.com/${dockerProject}/tdengine-enterprise-${dockername}:latest
  docker push image.cloud.taosdata.com/${dockerProject}/tdengine-enterprise-${dockername}:latest
fi

rm -f ${pkgFile}
