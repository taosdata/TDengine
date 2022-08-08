#!/bin/bash
#set -x
scriptDir=$(dirname $(realpath $0 || readlink -f $0))
#
version=$1
versionComp=$2
branchName=$3
verType=$4
cpuType=$5

topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
archiveDir=/nas/TDengine/v$version/enterprise  # version’package directory
enterpriseDir=$topDir/enterprise
allocator=jemalloc              # glibc  or  jemalloc, default is jemalloc

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir || echo -e "failed to create $archiveDir"
fi

echo "generate enterprise package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# cd $communityDir
# # git checkout $branchName
# # git checkout -- .
# # git pull
# # git checkout -- .
# rm -rf release/*

# cd $enterpriseDir
# # git checkout $branchName
# # git checkout -- .
# # git pull

cd $communityDir
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*
./packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

# if [ ! -d  "$archiveDir/v$version" ]; then
#   mkdir -p "$archiveDir/v$version"
# fi

# if [ ! -d enterprise ]; then
#     mkdir enterprise
# fi

# cd enterprise

#echo "build new version branch >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#cd $enterpriseDir
#git branch -d release/v$version
#git checkout -b release/v$version
#git merge master
#git push origin release/v$version

# modify tar.gz to append taoskeeper
cd $communityDir/release

server_tar=$(ls *-enterprise-server-*.tar.gz)
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
taoskeeper_binary=`$scriptDir/build_taoskeeper.sh --arch $arch`

set -e
# unpack server package and repack with taoskeeper binary and service file.
prefix=$(echo $server_tar |grep -Eo ".*-enterprise-server-[^\-]+")
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
echo "append taoskeeper to enterprise server package"
rm -rf $prefix/
rm -rf build-taoskeeper

# copy to nas [optional]
if [ -d $archiveDir ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./
else
    echo "Cannont found $archiveDir on this machine"
fi

