#!/bin/bash
#
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

echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $communityDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug
  cmake ..
fi

cd $communityDir
###git checkout $branchName
#git checkout -- .
#git pull
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*

# generate lite version in x64
if [ "$cpuType" == "x64" ]; then
  echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true"
  ./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -l lite -H true
fi

# need build lite package first. standard need rebuild to include blm3
echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType"
./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

# modify tar.gz to append taoskeeper
cd $communityDir/release

server_tar=$(ls *-server-*.tar.gz | grep -v Lite)
[ "$server_tar" == "" ] && exit # build taoskeeper only with server

echo "build taoskeeper"
if [ "$cpuType" = "x64" ] || [ "$cpuType" = "x86_64" ] || [ "$cpuType" = "amd64" ]; then
  arch=amd64
elif [ "$cpuType" = "x32" ] || [ "$cpuType" = "i386" ] || [ "$cpuType" = "i686" ]; then
  arch=386
elif [ "$cpuType" = "arm" ] || [ "$cpuType" = "aarch32" ]; then
  arch=arm
elif [ "$cpuType" = "arm64" ] || [ "$cpuType" = "aarch64" ]; then
  arch=arm64
else
  arch=$cpuType
fi
taoskeeper_binary=`$scriptDir/build_taoskeeper.sh --arch $arch --repo taoskeeper`

set -e
# unpack server package and repack with taoskeeper binary and service file.
prefix=$(echo $server_tar |grep -Eo ".*-server-[^\-]+")
tar axf $server_tar
[ -d "$prefix/taos" ] || mkdir $prefix/taos
tar axf $prefix/taos.tar.gz -C $prefix/taos/
cp -f $taoskeeper_binary $prefix/taos/bin/
cp -f $(dirname $taoskeeper_binary)/taoskeeper.service $prefix/taos/cfg/
cp -f $(dirname $taoskeeper_binary)/config/keeper.toml $prefix/taos/cfg/
cat $scriptDir/remove_taoskeeper.sh >> $prefix/taos/bin/remove.sh
cat $scriptDir/install_taoskeeper.sh >> $prefix/install.sh
cd $prefix/taos && tar acf ../taos.tar.gz ./ && cd ../../
rm -rf $prefix/taos $prefix/taos.tar
tar acf $server_tar $prefix
echo "append taoskeeper to community server package"
rm -rf $prefix/
rm -rf build-taoskeeper

# mv package to path:/nas/TDengine/version/
if [ -d $archiveDir ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./
    if [ "${cpuType}" == "x64" ]; then
        cp -f $communityDir/debs/* ./
        cp -f $communityDir/rpms/* ./
    fi
else
    echo "Cannont found $archiveDir on this machine"
fi

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $communityDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version

