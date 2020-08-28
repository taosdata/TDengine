#!/bin/bash
#
version=2.0.2.2
versionComp=2.0.0.0
branchName=develop
dockerPass="tbase125!"
tagVal=ver-${version}-beta

bash generate_community.sh  $version $versionComp $branchName
bash generate_enterprise.sh $version $versionComp $branchName
bash docker_generate.sh $version $daockerPass
bash tag.sh $tagVal $branchName
