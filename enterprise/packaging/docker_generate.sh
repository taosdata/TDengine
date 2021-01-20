#!/bin/bash
#
version=$1
password=$2
pkgFile=$3
cpuType=$4

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release

echo "make docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir/packaging/docker
cp -f $archiveDir/v$version/community/${pkgFile} .

./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${version} -p ${password}

rm -f ${pkgFile}
