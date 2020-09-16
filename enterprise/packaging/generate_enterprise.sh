#!/bin/bash
#
version=$1
versionComp=$2
branchName=$3
verType=$4

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
enterpriseDir=$topDir/enterprise
archiveDir=$scriptDir/../release

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir
fi

echo "generate enterprise package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
# git checkout $branchName
# git checkout -- .
git pull
git checkout -- .
rm -rf release/*

cd $enterpriseDir
# git checkout $branchName
# git checkout -- .
git pull

cd $communityDir
./packaging/release.sh -v cluster -n $version -m $versionComp -V $verType

if [ ! -d  "$archiveDir/v$version" ]; then
  mkdir -p "$archiveDir/v$version"
fi

cd $archiveDir/v$version
mkdir enterprise
cd enterprise
cp $communityDir/release/* ./

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $enterpriseDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version

