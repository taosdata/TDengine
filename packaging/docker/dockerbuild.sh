#!/bin/bash
set -e
#set -x

# dockerbuild.sh 
#             -c [aarch32 | aarch64 | amd64 | x86 | mips64 ...]  
#             -f [pkg file]
#             -n [version number]
#             -p [password for docker hub]

# set parameters by default value
cpuType=amd64
verNumber=""
passWord=""
pkgFile=""

while getopts "hc:n:p:f:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    n)
      #echo "verNumber=$OPTARG"
      verNumber=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    f)
      #echo "pkgFile=$OPTARG"
      pkgFile=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -c [aarch32 | aarch64 | amd64 | x86 | mips64 ...] "      
      echo "                      -f [pkg file] "      
      echo "                      -n [version number] "
      echo "                      -p [password for docker hub] "
      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "cpuType=${cpuType} verNumber=${verNumber} pkgFile=${pkgFile} "
echo "$(pwd)"
echo "====NOTES: ${pkgFile} must be in the same directory as dockerbuild.sh===="

dirName=${pkgFile%-Linux*}
#echo "dirName=${dirName}"

docker build --rm -f "Dockerfile" -t tdengine/tdengine-${cpuType}:${verNumber} "." --build-arg pkgFile=${pkgFile} --build-arg dirName=${dirName}
docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
docker push tdengine/tdengine-${cpuType}:${verNumber}

# set this version to latest version
docker tag tdengine/tdengine-${cpuType}:${verNumber} tdengine/tdengine-${cpuType}:latest
docker push tdengine/tdengine-${cpuType}:latest
