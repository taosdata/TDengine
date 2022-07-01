#!/bin/bash

# follow link: https://scan.coverity.com/download

workdir=/root
token="replace_with_coverity_scan_token"


cd $workdir/TDengine

git reset --hard HEAD
git checkout -- .
git checkout develop
git pull
git submodule update --init --recursive

mkdir -p debug
cd debug
rm -rf *
cmake .. -DBUILD_TOOLS=true
cov-build --dir cov-int make -j 4
tar czvf TDengine.tgz cov-int
curl --form token=$token \
	--form email="support@taosdata.com" \
	--form file=@TDengine.tgz \
	--form version="2.4.0.0" \
	--form description="TDengine Test" \
	https://scan.coverity.com/builds?project=tdengine
