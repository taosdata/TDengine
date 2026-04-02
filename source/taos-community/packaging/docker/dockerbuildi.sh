#!/bin/bash
#

set -e
#set -x

# dockerbuild.sh 
#             -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...]
#             -n [version number]
#             -p [password for docker hub]

# set parameters by default value
cpuType=aarch64
verNumber=""
passWord=""

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
    h)
      echo "Usage: `basename $0` -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...] "
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

pkgFile=TDengine-server-${verNumber}-Linux-${cpuType}.tar.gz

echo "cpuType=${cpuType} verNumber=${verNumber} pkgFile=${pkgFile} "

scriptDir=`pwd`
pkgDir=$scriptDir/../../release/

cp -f ${pkgDir}/${pkgFile} .

./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${verNumber} -p ${passWord}

rm -f ${pkgFile}
