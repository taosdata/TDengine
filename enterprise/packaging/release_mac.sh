#!/bin/bash

set -e

scriptDir=$(dirname $(readlink -f $0))
topDir=$scriptDir/../..         # TDinternal
communityDir=$topDir/community
allocator=glibc  

# new_ver_release.sh  -b [develop | master] 
#                     -c [aarch32 | aarch64 | x64 ...]  
#                     -n [2.1.*.* | 2.0.*.* ]
#                     -l [full | lite] 
#                     -v [cluster, edge ,all] cluster is enterprise, edge is community
#                     -V [stable | beta]
#                     -d [isdocker ] 
#                     -h help

# set parameters by default value
branchName=master   # -b [develop | master ]
cpuType=x64         # -c [aarch32 | aarch64 | x64 ...]
version="3.0.0.0"   # -n [2.1.*.* | 2.0.*.* ]
pagMode=full        # -l [full | lite] 
verMode=all         # -v [cluster, edge ,all ] cluster is enterprise, edge is community
verType=stable      # -V [stable, beta]
versionComp=3.0.0.0
dockerMode="no"

while getopts "hb:c:n:l:v:d:V:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    b)
      #echo "branchName=$OPTARG"
      branchName=$(echo $OPTARG)
      ;;
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    l)
      #echo "pagMode=$OPTARG"
      pagMode=$(echo $OPTARG)
      ;;
    v)
      #echo "verMode=$OPTARG"
      verMode=$(echo $OPTARG)
      ;;
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
      ;;
    d)
      #echo "dockerMode=$OPTARG"
      dockerMode=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master] "
      echo "                     -c [aarch32 | aarch64 | x64 ...] "
      echo "                     -n [version number: 2.1.*.* | 2.0.*.* ]      "
      echo "                     -l [full | lite]  "
      echo "                     -v [cluster, edge ,all] cluster is enterprise, edge is community  "
      echo "                     -V [stable | beta] "
      echo "                     -d [no | build | push | latest]   "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

# expect -c "spawn sudo ls; expect \"Password:\"; send -- \"$password\r\"; interact"

echo "generate commnunity package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $communityDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug
  cmake ..
fi

echo "build taoskeeper..."
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

taoskeeper_binary=`./build_taoskeeper.sh -r $arch -e taoskeeper`

cd $communityDir
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*

expect -c "spawn su root; expect \"Password:\"; send -- \"$password\r\"; interact"

echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType"
./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

sed -i '' "s/TDengine-client-3.0.1.4-macOS-arm64/TDengine-client-3.0.1.5-macOS-arm64/g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' 's/3.0.1.4/3.0.1.5/g' $communityDir/packaging/tools/TDengine.pkgproj

/usr/local/bin/packagesbuild --package-version $version ./packaging/tools/TDengine.pkgproj


exit 1
prefix="/opt"
cp -f $taoskeeper_binary $prefix/taos/bin/
cp -f $(dirname $taoskeeper_binary)/taoskeeper.service $prefix/taos/cfg/
cp -f $(dirname $taoskeeper_binary)/config/keeper.toml $prefix/taos/cfg/
cat $scriptDir/remove_taoskeeper.sh >> $prefix/taos/bin/remove.sh
cat $scriptDir/install_taoskeeper.sh >> $prefix/install.sh

echo "append taoskeeper to community server package"
rm -rf $prefix/
rm -rf build-taoskeeper