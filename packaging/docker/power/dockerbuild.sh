#!/bin/bash
set -x
$1
docker build --rm -f "Dockerfile" -t zdpf/powerdb:$1 "."
docker login -u zdpf -p tbase125!  #replace the docker registry username and password
docker push zdpf/powerdb:$1