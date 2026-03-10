#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -b target branch (default 3.0)"
    echo -e "\t -c target branch commit id"
    echo -e "\t -m branch to merge"
    echo -e "\t -n without build source"
    echo -e "\t -e TDinternal PR"
    echo -e "\t -h help"
}

enterprise=0
target_branch=3.0
build_src=1
while getopts "c:b:m:neh" opt; do
    case $opt in
        b)
            target_branch=$OPTARG
            ;;
        c)
            commit_id=$OPTARG
            ;;
        m)
            merge_branch=$OPTARG
            ;;
        n)
            build_src=0
            ;;
        e)
            enterprise=1
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done

echo "target branch: $target_branch"
echo "commit id:     $commit_id"
echo "merge branch:  $merge_branch"
echo "build source:  $build_src"
echo "enterprise:    $enterprise"
echo

set -x
TDINTERNAL_ROOT=/home/TDinternal
COMMUNITY_ROOT=$TDINTERNAL_ROOT/community
PYTHON_CONNECTOR_ROOT=/home/taos-connector-python

# prepare coredump directory
cat /proc/sys/kernel/core_pattern
coredump_dir=`cat /proc/sys/kernel/core_pattern|xargs dirname`
mkdir -p $coredump_dir

# prepare source code
cd $TDINTERNAL_ROOT
git checkout $target_branch
git fetch
cd $COMMUNITY_ROOT
git checkout $target_branch
git fetch

if [ $enterprise -ne 0 ]; then
    # TDinternal PR
    cd $TDINTERNAL_ROOT
else
    # TDengine PR
    cd $COMMUNITY_ROOT
fi


# checkout commit id
if [ ! -z $commit_id ]; then
    git checkout $commit_id
fi

# merge branch
if [ ! -z $merge_branch ]; then 
    git merge $merge_branch --no-edit
fi

cd $COMMUNITY_ROOT

if [ $build_src -ne 0 ]; then
    # build
    cd $TDINTERNAL_ROOT

    rm -rf debug
    mkdir -p debug

    cd debug

    cmake .. -DBUILD_TOOLS=true

    make -j 8

fi
rm -rf /usr/lib/libtaos.so
ln -s $TDINTERNAL_ROOT/debug/build/lib/libtaos.so /usr/lib/libtaos.so

# install python connector
echo "install python connector"
cd $PYTHON_CONNECTOR_ROOT
git pull
pip3 install .