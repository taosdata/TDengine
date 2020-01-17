#!/bin/bash
set -x
$1
tar -zxf $1
DIR=`echo $1|awk -F . '{print($1"."$2"."$3"."$4)}'`
mv $DIR tdengine
tar -czf tdengine.tar.gz tdengine
TMP=`echo $1|awk -F . '{print($2"."$3"."$4)}'`
TAG="1."$TMP
docker build --rm -f "Dockerfile" -t tdengine/tdengine:$TAG "."
docker login -u tdengine -p ********  #replace the docker registry username and password
docker push tdengine/tdengine:$TAG