#!/bin/bash
#

set -e
#set -x

# dockerbuild.sh 
#             -c [aarch32 | aarch64 | amd64 | x86 | mips64 ...]  
#             -n [version number]
#             -p [password for docker hub]
#             -V [stable | beta]
#             -f [pkg file]

# set parameters by default value
cpuType=""
version=""
passWord=""
pkgFile=""
verType="stable"

while getopts "hc:n:p:f:V:" arg
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
    h)
      echo "Usage: `basename $0`  -c [aarch32 | aarch64 | amd64 | x86 | mips64 ...] "
      echo "                      -n [version number] "
      echo "                      -p [password for docker hub] "
      echo "                      -V [stable | beta] "
      echo "                      -f [pkg file] "

      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

# if [ "$verType" == "beta" ]; then
#   pkgFile=TDengine-server-${version}-Linux-${cpuType}-${verType}.tar.gz
# elif [ "$verType" == "stable" ]; then
#   pkgFile=TDengine-server-${version}-Linux-${cpuType}.tar.gz
# else
#   echo "unknow verType, nor stabel or beta"
#   exit 1

if [ "$verType" == "beta" ]; then
  dockername=${cpuType}-${verType}
  dirName=${pkgFile%-beta*}
elif [ "$verType" == "stable" ]; then
  dockername=${cpuType}
  dirName=${pkgFile%-Linux*}
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi


echo "cpuType=${cpuType} version=${version} pkgFile=${pkgFile} verType=${verType} "
echo "$(pwd)"
echo "====NOTES: ${pkgFile} must be in the same directory as dockerbuild.sh===="

scriptDir=$(dirname $(readlink -f $0))
comunityArchiveDir=/nas/TDengine/v$version/community   # community version’package directory
cd ${scriptDir}
cp -f ${comunityArchiveDir}/${pkgFile}  .

echo "dirName=${dirName}"


docker build --rm -f "Dockerfile"  --network=host -t tdengine/tdengine-${dockername}:${version} "." --build-arg pkgFile=${pkgFile} --build-arg dirName=${dirName}
docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
docker push tdengine/tdengine-${dockername}:${version}

# set this version to latest version
if  [ "$branchName" == "2.4" ] || [ "$branchName" == "develop" ]  ;then
  docker tag tdengine/tdengine-${dockername}:${version} tdengine/tdengine-${dockername}:latest
  docker push tdengine/tdengine-${dockername}:latest
fi



rm -f ${pkgFile}