#!/bin/bash
#
version=2.0.1.1
dockerPass="tbase125!"

bash generate_community.sh $version
bash generate_enterprise.sh $version
bash docker_generate.sh $version $daockerPass
bash tag.sh $version
