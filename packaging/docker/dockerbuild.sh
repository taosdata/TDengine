#!/bin/bash
set -x
$1
docker build --rm -f "Dockerfile" -t tdengine/tdengine:$1 "."
docker login -u tdengine -p $2  #replace the docker registry username and password
docker push tdengine/tdengine:$1
