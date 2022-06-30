#!/bin/bash
#
#set -x

version=$1
versionComp=$2
branchName=$3
verType=$4
cpuType=$5

scriptDir=$(dirname $(readlink -f $0))
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=/nas/TDengine/v$version/community # version’package directory
allocator=glibc                 # glibc  or  jemalloc

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir || echo -e "failed to create $archiveDir"
fi

echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
###git checkout $branchName
#git checkout -- .
#git pull
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*

# generate lite version in x64
if [ "$cpuType" == "x64" ]; then
  echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true"
  ./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true
fi

# need build lite package first. standard need rebuild to include blm3
echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType"
./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType


# mv package to path:/nas/TDengine/version/
if [ -d $archiveDir ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./
    if [ "${cpuType}" == "x64" ]; then
        cp -f $communityDir/debs/* ./
        cp -f $communityDir/rpms/* ./
    fi
else
    echo "Cannont found $archiveDir on this machine"
fi

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $communityDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version

