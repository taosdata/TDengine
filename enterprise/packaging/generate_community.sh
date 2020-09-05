#!/bin/bash
#
version=$1
versionComp=$2
branchName=$3
verType=$4

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir
fi

echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
###git checkout $branchName
git checkout -- .
git pull
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*
./packaging/release.sh -n $version -m $versionComp -V $verType
cd $archiveDir
rm -rf v$version
mkdir v$version
cd v$version
mkdir community
cd community
cp $communityDir/release/* ./
cp $communityDir/debs/* ./
cp $communityDir/rpms/* ./

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $communityDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version
