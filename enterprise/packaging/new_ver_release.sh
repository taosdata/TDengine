#!/bin/bash
#
version=2.0.13.0
versionComp=2.0.0.0

## master
branchName=release/s111
verType=stable
dockerPass="tbase125!"
tagVal=ver-${version}
dockerinput=TDengine-server-${version}-Linux-x64.tar.gz

## develop
# branchName=develop
# verType=beta
# dockerPass="tbase125!"
# tagVal=ver-${version}-beta
# dockerinput=TDengine-server-${version}-Linux-x64-beta.tar.gz

bash generate_community.sh  $version $versionComp $branchName $verType
bash generate_enterprise.sh $version $versionComp $branchName $verType
#bash docker_generate.sh $version $dockerPass $dockerinput
#bash tag.sh $tagVal $branchName
