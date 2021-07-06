#!/bin/bash
#
version=$1
password=$2
branchName=$3
verType=$4
cpuType=$5
pkgFile=$6

scriptDir=$(dirname $(readlink -f $0))
topDir=${scriptDir}/../..         # TDinternal
communityDir=${topDir}/community  # community
archiveDir=/nas/TDengine/v${version}/community # version’package directory
cd ${communityDir}/packaging/docker
echo "make docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# cd $communityDir/packaging/docker
# cp -f $archiveDir/${pkgFile} .

./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${version} -p ${password} -V ${verType}

rm -f ${pkgFile}
