#!/bin/bash
#
tagVal=$1
branchName=$2

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
enterpriseDir=$topDir/enterprise

echo "put tag on community master branch>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
git checkout $branchName
git tag -d $tagVal
git push origin --delete tag $tagVal
git tag -a $tagVal      # open vim and input release notes
git show-ref --tags
git push origin $tagVal

echo "put tag on enterprise master branch>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $enterpriseDir
git checkout $branchName
git tag -d $tagVal
git push origin --delete tag $tagVal
git tag -a $tagVal      # open vim and input release notes
git show-ref --tags
git push origin $tagVal
