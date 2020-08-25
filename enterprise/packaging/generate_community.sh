#!/bin/bash
#
version=$1
scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release
dockerinput=TDengine-${version}.tar.gz

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir
fi

echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
git checkout master
git checkout -- .
git pull
sudo rm -rf release/*
sudo rm -rf debs/*
sudo rm -rf rpms/*
sudo ./packaging/release.sh -n $version
cd $archiveDir
rm -rf v$version
mkdir v$version
cd v$version
mkdir community
cd community
cp $communityDir/release/* ./
cp $communityDir/debs/* ./
cp $communityDir/rpms/* ./

echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
git branch -d release/v$version
git checkout -b release/v$version
git merge master
git push origin release/v$version
