#! /bin/bash

set -x

cd $1
git reset --hard HEAD
git checkout -- .
git checkout $2
git pull

sed -i ':a;N;$!ba;s/\(.*\)OFF/\1ON/' $1/cmake/cmake.options

mkdir -p $1/debug
rm -rf $1/debug/*
cd $1/debug
cmake .. -DBUILD_TOOLS=true
cd $1/debug
make -j 4
cd $1/debug
make install 

systemctl start taosd
