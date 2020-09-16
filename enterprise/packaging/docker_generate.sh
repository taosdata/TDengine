#!/bin/bash
#
version=$1
password=$2
dockerinput=$3

scriptDir=`pwd`
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=$scriptDir/../release

echo "make docker for community version >>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
cd $communityDir/packaging/docker
cp -f $archiveDir/v$version/community/${dockerinput} ./tdengine.tar.gz
./dockerbuild.sh ${version} ${password}
echo ">>>>>>>>>>>>> check whether the docker image has been published"
docker pull tdengine/tdengine:${version}

docker tag tdengine/tdengine:$version tdengine/tdengine:latest
docker push tdengine/tdengine:latest


