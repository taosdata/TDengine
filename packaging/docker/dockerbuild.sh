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
cpuTypeAlias=""
version=""
passWord=""
pkgFile=""
verType="stable"
dockerLatest="n"

while getopts "hc:n:p:f:V:a:b:" arg
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
    a)
      #echo "dockerLatest=$OPTARG"
      dockerLatest=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0`  -c [aarch32 | aarch64 | amd64 | x86 | mips64 ...] "
      echo "                      -n [version number] "
      echo "                      -p [password for docker hub] "
      echo "                      -V [stable | beta] "
      echo "                      -f [pkg file] "
      echo "                      -a [y | n ]   "
      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done


# Check_verison()
# {
# }


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
communityDir=${scriptDir}/../../../community
DockerfilePath=${communityDir}/packaging/docker/
Dockerfile=${communityDir}/packaging/docker/Dockerfile
cd ${scriptDir}
cp -f ${comunityArchiveDir}/${pkgFile}  .

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

docker build --rm -f "${Dockerfile}"  --network=host -t tdengine/tdengine-${dockername}:${version} "." --build-arg pkgFile=${pkgFile} --build-arg dirName=${dirName} --build-arg cpuType=${cpuTypeAlias}
docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
docker push tdengine/tdengine-${dockername}:${version}

if [ -n "$(docker ps -aq)" ] ;then 
  echo "delete docker process"
  docker stop $(docker ps -aq)
  docker rm $(docker ps -aq)
fi

if [  -n "$(pidof taosd)" ] ;then
   echo "kill taosd "
   kill -9 $(pidof taosd)
fi

if [ -n "$(pidof power)" ]  ;then
  echo "kill power "
  kill -9 $(pidof power)
fi


echo ">>>>>>>>>>>>> check whether  tdengine/tdengine-${dockername}:${version}  has been published"
docker run -d --name doctest -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp  tdengine/tdengine-${dockername}:${version}
sleep 2
curl -u root:taosdata -d 'show variables;'  127.0.0.1:6041/rest/sql > temp1.data
data_version=$( cat temp1.data |jq .data| jq '.[]' |grep "version" -A 2 -B 1  | jq ".[1]")
echo "${data_version}" 
if [ "${data_version}" == "\"${version}\"" ] ; then
    echo  "docker version is right "
else
    echo  "docker version is wrong "
    exit 1
fi
rm -rf  temp1.data

# set this version to latest version 
if  [ ${dockerLatest} == 'y' ]  ;then
  docker tag tdengine/tdengine-${dockername}:${version} tdengine/tdengine-${dockername}:latest
  docker push tdengine/tdengine-${dockername}:latest
  echo ">>>>>>>>>>>>> check whether  tdengine/tdengine-${dockername}:latest has been published correctly"
  docker run -d --name doctestla -p 7030-7049:6030-6049 -p 7030-7049:6030-6049/udp   tdengine/tdengine-${dockername}:latest 
  sleep 2
  curl -u root:taosdata -d 'show variables;'  127.0.0.1:7041/rest/sql >  temp2.data
  version_latest=` cat temp2.data |jq .data| jq '.[]' |grep "version" -A 2 -B 1  | jq ".[1]" `
  echo "${version_latest}" 
  if [ "${version_latest}" == "\"${version}\"" ] ; then
      echo  "docker version is right "
  else 
      echo  "docker version is wrong "
      exit 1 
  fi
fi
rm -rf  temp2.data

if [ -n "$(docker ps -aq)" ] ;then 
  echo "delte docker process"
  docker stop $(docker ps -aq)
  docker rm $(docker ps -aq)
fi

cd ${scriptDir}
rm -f ${pkgFile}
