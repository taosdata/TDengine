#!/bin/bash

if [ x$1 != x ] && [ x$2 != x ];then        
        echo ": start preparation for release: $1 on $2 branch"
else
        echo "usage: ./prepare_release.sh version_number branch_name (eg: ./prepare_release.sh 3.2.0.0 main)"
        exit 1
fi

TOP_DIR=`pwd`

echo "create dir: $TOP_DIR/$1"
mkdir -p  $TOP_DIR/$1

components=(grant-lib taoskeeperinternal taoskeeper taosadapter taosx explorer taos-tools)

branch=$2

for component in "${components[@]}"; do
	if [[ "$component" == "taoskeeperinternal" || "$component" == "taoskeeper"  || "$component" == "taosx"  || "$component" == "explorer" || "$component" == "taos-tools" ]];then
		branch="main"
	fi
    cd $TOP_DIR/$1	
	if [ -d $TOP_DIR/$1/$component ]; then
		cd $TOP_DIR/$1/$component
		git tag -d ver-$1 ||:
		git push origin --delete tag ver-$1 ||:
		git checkout $branch
		git pull
	else
		git clone -b $branch --depth=1 https://github.com/taosdata/$component.git		
	fi
	cd $TOP_DIR/$1/$component
	git tag ver-$1
	git push origin ver-$1
done

cd $TOP_DIR/$1
if [ -d $TOP_DIR/$1/TDengine ]; then
	cd $TOP_DIR/$1/TDengine
	git tag -d ver-$1 ||:
	git push origin --delete tag ver-$1 ||:
	git checkout $branch
	git branch -D release/ver-$1 ||:
	git push origin --delete release/ver-$1 ||:
	git pull
else
	git clone -b $branch --depth=1 https://github.com/taosdata/TDengine.git
fi
cd $TOP_DIR/$1/TDengine
git checkout -b release/ver-$1

sed -i "s/GIT_TAG $branch/GIT_TAG ver-$1/g" cmake/taosadapter_CMakeLists.txt.in
sed -i "s/GIT_TAG main/GIT_TAG ver-$1/g" cmake/taostools_CMakeLists.txt.in
sed -i "s/\(TD_VER_NUMBER\) \"[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*\"/\1 \"$1\"/g" cmake/cmake.version
git diff
git add cmake/cmake.version cmake/taosadapter_CMakeLists.txt.in cmake/taostools_CMakeLists.txt.in
git commit -m "build: release ver-$1"
git push origin release/ver-$1
git tag ver-$1
git push origin ver-$1
	
cd $TOP_DIR/$1

if [ -d $TOP_DIR/$1/TDinternal ]; then
	cd $TOP_DIR/$1/TDinternal
	git tag -d ver-$1 ||:
	git push origin --delete tag ver-$1 ||:
	git checkout $branch
	git branch -D release/ver-$1 ||:
	git push origin --delete release/ver-$1 ||:
	git pull
else
	git clone -b $branch --depth=1 https://github.com/taosdata/TDinternal.git
fi
cd $TOP_DIR/$1/TDinternal
git checkout -b release/ver-$1

sed -i "s/GIT_TAG $branch/GIT_TAG ver-$1/g" enterprise/cmake/community_CMakeLists.txt.in
sed -i "s/GIT_TAG 3.2/GIT_TAG ver-$1/g" enterprise/cmake/grant-lib_CMakeLists.txt.in
sed -i "s/GIT_TAG main/GIT_TAG ver-$1/g" enterprise/cmake/taosx_CMakeLists.txt.in
sed -i "s/GIT_TAG main/GIT_TAG ver-$1/g" enterprise/cmake/explorer_CMakeLists.txt.in
git diff
git add .
git commit -m "build: release ver-$1"
git push origin release/ver-$1
git tag ver-$1
git push origin ver-$1

echo "preparation for release $1 is completed!"
