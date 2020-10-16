#!/bin/bash
set -x
docker build --rm -f "Dockerfile" -t tdengine/tdengine:$1 "." --build-arg version=$1
docker login -u tdengine -p $2  #replace the docker registry username and password
docker push tdengine/tdengine:$1
