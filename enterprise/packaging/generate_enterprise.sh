#!/bin/bash
#
version=$1
versionComp=$2
branchName=$3
verType=$4
cpuType=$5


scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=/nas/TDengine/v$version/enterprise  # version’package directory
enterpriseDir=$topDir/enterprise
allocator=jemalloc              # glibc  or  jemalloc, default is jemalloc

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir
fi

echo "generate enterprise package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# cd $communityDir
# # git checkout $branchName
# # git checkout -- .
# # git pull
# # git checkout -- .
# rm -rf release/*

# cd $enterpriseDir
# # git checkout $branchName
# # git checkout -- .
# # git pull

cd $communityDir
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*
git submodule update --init --recursive
./packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

# if [ ! -d  "$archiveDir/v$version" ]; then
#   mkdir -p "$archiveDir/v$version"
# fi

cd $archiveDir

# if [ ! -d enterprise ]; then
#     mkdir enterprise
# fi

# cd enterprise
cp  -f $communityDir/release/* ./

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $enterpriseDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version

