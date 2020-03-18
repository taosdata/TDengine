#!/bin/bash
set -x
echo input tag: $1
sudo docker build --rm -f "Dockerfile" -t zdpf/powerdb:$1 "."
sudo docker login -u zdpf -p tbase125!  #replace the docker registry username and password
sudo docker push zdpf/powerdb:$1
