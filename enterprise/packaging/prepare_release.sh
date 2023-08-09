#!/bin/bash

if [ x$1 != x ];then
        version=$1
        echo ": start preparation for release: $version"
else
        echo "Please enter release number"
        exit 1
fi

TOP_DIR=`pwd`

echo "create dir: $TOP_DIR/$1"
mkdir -p  $TOP_DIR/$1

cd $TOP_DIR/$1

if [ -d $TOP_DIR/$1/grant-lib ]; then
	cd $TOP_DIR/$1/grant-lib
	git tag -d ver-$1
	git push origin --delete tag ver-$1
	git checkout main
	git pull
else
	git clone -b main --depth=1 https://github.com/taosdata/grant-lib.git
fi
cd $TOP_DIR/$1/grant-lib
git tag ver-$1
git push origin ver-$1


if [ -d $TOP_DIR/$1/TDengine ]; then
	cd $TOP_DIR/$1/TDengine
	git tag -d ver-$1
	git push origin --delete tag ver-$1
	git checkout main
	git branch -D release/ver-$1
	git push origin --delete release/ver-$1
	git pull
else
	git clone -b main --depth=1 https://github.com/taosdata/TDengine.git
fi
cd $TOP_DIR/$1/TDengine
git checkout -b release/ver-$1
sed -i "s/\(TD_VER_NUMBER\) \"[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\.alpha\"/\1 \"$1\"/g" cmake/cmake.version
git diff
git add cmake/cmake.version
git commit -m "build: release ver-$1"
git push origin release/ver-$1
git tag ver-$1
git push origin ver-$1
	

if [ -d $TOP_DIR/$1/TDinternal ]; then
	cd $TOP_DIR/$1/TDinternal
	git tag -d ver-$1
	git push origin --delete tag ver-$1
	git checkout main
	git branch -D release/ver-$1
	git push origin --delete release/ver-$1
	git pull
else
	git clone -b main --depth=1 https://github.com/taosdata/TDengine.git
fi
cd $TOP_DIR/$1/TDinternal
git checkout -b release/ver-$1
sed -i "s/GIT_TAG main/GIT_TAG ver-$1/g" enterprise/cmake/community_CMakeLists.txt.in
sed -i "s/GIT_TAG main/GIT_TAG ver-$1/g" enterprise/cmake/grant-lib_CMakeLists.txt.in
git diff
git add enterprise/cmake/community_CMakeLists.txt.in enterprise/cmake/grant-lib_CMakeLists.txt.in
git commit -m "build: release ver-$1"
git push origin release/ver-$1
git tag ver-$1
git push origin ver-$1


echo "preparation for release $1 is completed!"
