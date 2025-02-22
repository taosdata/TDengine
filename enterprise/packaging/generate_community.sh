#!/bin/bash
#
set -e
#set -x

version=$1
versionComp=$2
branchName=$3
verType=$4
cpuType=$5

scriptDir=$(dirname $(readlink -f $0))
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=/nas/TDengine/v$version/community # version’package directory
allocator=glibc                 # glibc  or  jemalloc

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir || echo -e "failed to create $archiveDir"
fi

echo "generate community package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# if [ ! -d $communityDir ]; then
#   cd $topDir
#   mkdir -p debug
#   cd debug
#   cmake ..
# fi

cd $communityDir

function git_checkout_operations {
  local dir=$1
  cd $dir

  if ! git fetch ; then
    echo "Failed to fetch latest changes in $dir"
    exit 1
  fi
  
  if ! git checkout $branchName; then
    echo "Failed to checkout branch $branchName in $dir"
    exit 1
  fi

  # do not discard changes in the directory
  # if ! git checkout -- .; then
  #   echo "Failed to discard changes in $dir"
  #   exit 1
  # fi

  # 检查 branchName 是否为 tag
  if git show-ref --tags | grep -q "refs/tags/$branchName$"; then
    echo "$branchName is a tag, skipping git pull"
  else
    if ! git pull; then
      echo "Failed to pull latest changes in $dir"
      exit 1
    fi
  fi
}

# 对 community 目录执行切换分支操作
git_checkout_operations ${communityDir}

rm -rf release/*
rm -rf debs/*
rm -rf rpms/*

if [ "$cpuType" == "x64" ]; then
  allocator=glibc
fi
# generate lite version in x64
if [ "$cpuType" == "x64" ]; then
  echo "../enterprise/packaging/release.sh -a glibc -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true"
  ../enterprise/packaging/release.sh -a glibc -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true
fi

# need build lite package first. standard need rebuild to include blm3
echo "../enterprise/packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType"
../enterprise/packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

# modify tar.gz to append taoskeeper
cd $communityDir/release

server_tar=$(ls *-server-*.tar.gz | grep -v Lite)
[ "$server_tar" == "" ] && exit # build taoskeeper only with server

# echo "build taoskeeper"
# if [ "$cpuType" = "x64" ] || [ "$cpuType" = "x86_64" ] || [ "$cpuType" = "amd64" ]; then
#   arch=amd64
# elif [ "$cpuType" = "x32" ] || [ "$cpuType" = "i386" ] || [ "$cpuType" = "i686" ]; then
#   arch=386
# elif [ "$cpuType" = "arm" ] || [ "$cpuType" = "aarch32" ]; then
#   arch=arm
# elif [ "$cpuType" = "arm64" ] || [ "$cpuType" = "aarch64" ]; then
#   arch=arm64
# elif [ "$cpuType" = "mips64" ]; then
#   arch=mips64le
# else
#   arch=$cpuType
# fi
# taoskeeper_binary=`$scriptDir/build_taoskeeper.sh -r $arch -e taoskeeper -t ver=$version`

# set -e
# # unpack server package and repack with taoskeeper binary and service file.
# prefix=$(echo $server_tar |grep -Eo ".*-server-[^\-]+")
# tar axf $server_tar
# [ -d "$prefix/taos" ] || mkdir $prefix/taos
# tar axf $prefix/package.tar.gz -C $prefix/taos/
# cp -f $taoskeeper_binary $prefix/taos/bin/
# cp -f $(dirname $taoskeeper_binary)/taoskeeper.service $prefix/taos/cfg/
# cp -f $(dirname $taoskeeper_binary)/config/taoskeeper.toml $prefix/taos/cfg/
# cd $prefix/taos && tar acf ../package.tar.gz ./ && cd ../../
# rm -rf $prefix/taos $prefix/package.tar
# tar acf $server_tar $prefix
# echo "append taoskeeper to community server package"
# rm -rf $prefix/
# rm -rf build-taoskeeper

osName=`cat /etc/os-release |grep ^NAME=|awk -F '=' '{print $2}'`
# mv package to path:/nas/TDengine/version/
if [ -d $archiveDir ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./
    if [[ "${cpuType}" == "x64"  &&  $osName == "\"Ubuntu\"" ]]; then
        cp -f $communityDir/rpms/* ./
        cp -f $communityDir/debs/* ./
        echo "build rpms and debs package at Ubuntu"
    elif [[ "${cpuType}" == "x64"  &&  $osName == "\"CentOS Linux\"" ]]; then
        cp -f $communityDir/rpms/* ./
        cp -f $communityDir/debs/* ./
        echo "build rpms package at CentOS Linux"
    fi
else
    echo "Cannot found $archiveDir on this machine"
fi

echo " packaging release done! "
#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $communityDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version