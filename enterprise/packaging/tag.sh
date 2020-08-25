#!/bin/bash
#
version=$1
scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
enterpriseDir=$topDir/enterprise

echo "put tag on community master branch>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
git checkout master
git tag -d ver-$version
git push origin --delete tag ver-$version
git tag -a ver-$version      # open vim and input release notes
git show-ref --tags
git push origin ver-$version

echo "put tag on enterprise master branch>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $enterpriseDir
git checkout master
git tag -d ver-$version
git push origin --delete tag ver-$version
git tag -a ver-$version      # open vim and input release notes
git show-ref --tags
git push origin ver-$version

