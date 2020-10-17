#!/bin/bash
#
version=2.0.5.1
versionComp=2.0.0.0

## master
branchName=develop
verType=stable
cpuType=aarch64
dockerPass="tbase125!"
tagVal=ver-${version}
dockerinput=TDengine-server-${version}-Linux-aarch64.tar.gz

## develop
# branchName=develop
# verType=beta
# dockerPass="tbase125!"
# tagVal=ver-${version}-beta
# dockerinput=TDengine-server-${version}-Linux-aarch64-beta.tar.gz

#bash generate_community.sh  $version $versionComp $branchName $verType
#bash generate_enterprise.sh $version $versionComp $branchName $verType
#bash docker_generate.sh $version $dockerPass $dockerinput


scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release
if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir
fi

####################### compile community version
echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
###git checkout $branchName
#git checkout -- .
#git pull
rm -rf release/*
./packaging/release.sh -n $version -m $versionComp -V $verType -c $cpuType
cd $archiveDir
rm -rf v$version
mkdir v$version
cd v$version
mkdir community
cd community
cp $communityDir/release/* ./

####################### compile enterprise version
echo "generate enterprise package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir
# git checkout $branchName
# git checkout -- .
# git pull
# git checkout -- .
rm -rf release/*

cd $enterpriseDir
# git checkout $branchName
# git checkout -- .
# git pull

cd $communityDir
./packaging/release.sh -v cluster -n $version -m $versionComp -V $verType -c $cpuType

if [ ! -d  "$archiveDir/v$version" ]; then
  mkdir -p "$archiveDir/v$version"
fi

cd $archiveDir/v$version
mkdir enterprise
cd enterprise
cp $communityDir/release/* ./

####################### build docker image and push
echo "make docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir/packaging/docker
cp -f $archiveDir/v$version/community/${dockerinput} ./tdengine.tar.gz
./dockerbuild-aarch64.sh ${version} ${dockerPass}
echo ">>>>>>>>>>>>> check whether the docker image has been published"
docker pull tdengine/tdengine-aarch64:${version}

docker tag tdengine/tdengine-aarch64:$version tdengine/tdengine-aarch64:latest
docker push tdengine/tdengine-aarch64:latest
