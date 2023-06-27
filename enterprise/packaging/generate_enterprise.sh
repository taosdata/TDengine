#!/bin/bash
set -e
# set -x
scriptDir=$(dirname $(realpath $0 || readlink -f $0))
#
version=$1
versionComp=$2
branchName=$3
verType=$4
cpuType=$5
grantValue=$6
cusName=$7
cusPrompt=$8
cusEmail=$9

topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
enterpriseDir=$topDir/enterprise
archiveDir=/nas/TDengine/v$version/enterprise  # version’package directory

ostype=$(uname)

if [ "${ostype}" == "Darwin" ] || [ "$cpuType" == "arm64" ]; then
    allocator=glibc
else
    allocator=glibc              # glibc  or  jemalloc, default is jemalloc
fi

if [ ! -d $archiveDir ]; then
  mkdir -p $archiveDir || echo -e "failed to create $archiveDir"
fi

echo "generate enterprise package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $communityDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug

  if [ -z "$cusName" ] && [ -z "$cusPrompt" ] && [ -z "$cusEmail" ]; then
    cmake .. -DBUILD_TAOSX=false
  else
    if [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ] && [ ! -z "$cusEmail" ]; then
      cmake .. -DBUILD_TAOSX=false -DCUS_NAME=${cusName} -DCUS_PROMPT=${cusPrompt} -DCUS_EMAIL=${cusEmail} -DGRANT_VALUE=${grantValue}
    elif [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ]; then
      cmake .. -DBUILD_TAOSX=false -DCUS_NAME=${cusName} -DCUS_PROMPT=${cusPrompt} -DGRANT_VALUE=${grantValue}
    elif [ ! -z "${cusName}" ]; then
      cmake .. -DBUILD_TAOSX=false -DCUS_NAME=${cusName} -DGRANT_VALUE=${grantValue} 
    else
      cmake .. -DBUILD_TAOSX=false -DCUS_PROMPT=${cusPrompt} -DGRANT_VALUE=${grantValue} 
    fi
  fi
fi

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

cd $topDir
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*


if [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ] && [ ! -z "$cusEmail" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -P ${cusPrompt} -M ${cusEmail} -G ${grantValue} 
elif [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -P ${cusPrompt} -G ${grantValue} 
elif [ ! -z "${cusName}" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -G ${grantValue} 
else
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -G ${grantValue} 
fi

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

taoskeeper_binary=`$scriptDir/build_taoskeeper.sh -r $arch -e taoskeeperinternal`

set -e
# unpack server package and repack with taoskeeper binary and service file.
prefix=$(echo $server_tar |grep -Eo ".*-enterprise-server-[^\-]+")
tar xf $server_tar
[ -d "$prefix/taos" ] || mkdir $prefix/taos
tar xf $prefix/package.tar.gz -C $prefix/taos/
cp -f $taoskeeper_binary $prefix/taos/bin/
cp -f $(dirname $taoskeeper_binary)/taoskeeper.service $prefix/taos/cfg/
cp -f $(dirname $taoskeeper_binary)/config/taoskeeper.toml $prefix/taos/cfg/
cat $scriptDir/remove_taoskeeper.sh >> $prefix/taos/bin/remove.sh
cat $scriptDir/install_taoskeeper.sh >> $prefix/install.sh
cd $prefix/taos && tar acf ../package.tar.gz ./ && cd ../../
rm -rf $prefix/taos
tar acf $server_tar $prefix
echo "append taoskeeper to enterprise server package"
rm -rf $prefix/
rm -rf build-taoskeeper

# copy to nas [optional]
if [ -d $archiveDir ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./
else
    echo "Cannot found $archiveDir on this machine"
fi

echo " packaging release done! "
