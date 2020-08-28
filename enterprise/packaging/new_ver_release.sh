#!/bin/bash
#
version=2.0.2.2
versionComp=2.0.0.0
branchName=develop
verType=beta
dockerPass="tbase125!"
tagVal=ver-${version}-beta

bash generate_community.sh  $version $versionComp $branchName $verType
bash generate_enterprise.sh $version $versionComp $branchName $verType
bash docker_generate.sh $version $daockerPass
bash tag.sh $tagVal $branchName
