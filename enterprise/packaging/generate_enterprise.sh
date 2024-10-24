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
skip=$7
cusName=$8
cusPrompt=$9
cusEmail=${10}

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
      cmake .. -DBUILD_TAOSX=true -DCUS_NAME=${cusName} -DCUS_PROMPT=${cusPrompt} -DCUS_EMAIL=${cusEmail} -DGRANT_VALUE=${grantValue}
    elif [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ]; then
      cmake .. -DBUILD_TAOSX=true -DCUS_NAME=${cusName} -DCUS_PROMPT=${cusPrompt} -DGRANT_VALUE=${grantValue}
    elif [ ! -z "${cusName}" ]; then
      cmake .. -DBUILD_TAOSX=true -DCUS_NAME=${cusName} -DGRANT_VALUE=${grantValue} 
    else
      cmake .. -DBUILD_TAOSX=true -DCUS_PROMPT=${cusPrompt} -DGRANT_VALUE=${grantValue} 
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

echo "./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -P ${cusPrompt} -M ${cusEmail} -G ${grantValue} -S ${skip}"
if [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ] && [ ! -z "$cusEmail" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -P ${cusPrompt} -M ${cusEmail} -G ${grantValue} -S ${skip}
elif [ ! -z "${cusName}" ] && [ ! -z "$cusPrompt" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -P ${cusPrompt} -G ${grantValue} -S ${skip}
elif [ ! -z "${cusName}" ]; then
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -N ${cusName} -G ${grantValue} -S ${skip}
else
    ./enterprise/packaging/release.sh -v cluster -a $allocator -n $version -m $versionComp -V $verType -c $cpuType -G ${grantValue} -S ${skip}
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
rm -rf build-taoskeeper

server_tar=$(ls *-enterprise-*.tar.gz | grep -v client)
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

if [ -z "$cusName" ] && [ -z "$cusPrompt" ] && [ -z "$cusEmail" ]; then
  taoskeeper_binary=`$scriptDir/build_taoskeeper.sh -r $arch -e taoskeeperinternal -t ver-$version`
else
  taoskeeper_binary=`$scriptDir/build_taoskeeper.sh -r $arch -e taoskeeperinternal -t ver-$version -N ${cusName} -M ${cusEmail} -P ${cusPrompt}`
fi

set -e
# unpack server package and repack with taoskeeper binary and service file.
prefix=$(echo $server_tar |grep -Eo ".*-enterprise-[^\-]+")
tar xf $server_tar
[ -d "$prefix/taos" ] || mkdir $prefix/taos
tar xf $prefix/package.tar.gz -C $prefix/taos/
cp -f $taoskeeper_binary $prefix/taos/bin/
cp -f $(dirname $taoskeeper_binary)/taoskeeper.service $prefix/taos/cfg/
cp -f $(dirname $taoskeeper_binary)/config/taoskeeper.toml $prefix/taos/cfg/
cd $prefix/taos && tar acf ../package.tar.gz ./ && cd ../../
rm -rf $prefix/taos
tar acf $server_tar $prefix
echo "append taoskeeper to enterprise server package"
rm -rf $prefix/
rm -rf build-taoskeeper

# copy TDengine package to nas [optional]
if [ -d $archiveDir ] && [ -z "${cusName}" ]; then
    cd $archiveDir
    cp -f $communityDir/release/* ./

    if [ $skip == 0 ]; then
      # copy client package to server if password free is set
      ssh root@taosdata.com -o PreferredAuthentications=publickey -o StrictHostKeyChecking=no "date" > /dev/null 2>&1
      if [ $? = 0 ]; then
        scp $communityDir/release/*client* root@taosdata.com:/data/www/assets-download/3.0/
        if [ $? > 0 ]; then
          echo "copy client package to taosdata server failed"
        fi
      fi
      
      ssh ubuntu@tdengine.com -o PreferredAuthentications=publickey -o StrictHostKeyChecking=no "date" > /dev/null 2>&1
      if [ $? = 0 ]; then
        scp $communityDir/release/*client* ubuntu@tdengine.com:/data/www/assets-download/3.0/
        if [ $? > 0 ]; then
          echo "copy client package to TDengine server failed"
        fi
      fi
    fi
else
    echo "Cannot find $archiveDir on this machine"
fi

echo " packaging release done! "
