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

echo "generate community package>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
if [ ! -d $communityDir ]; then
  cd $topDir
  mkdir -p debug
  cd debug
  cmake ..
fi

cd $communityDir
rm -rf release/*
rm -rf debs/*
rm -rf rpms/*

echo "./packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType"
$topDir/enterprise/packaging/release.sh -a $allocator -n $version -m $versionComp -V $verType -c $cpuType

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

taoskeeper_binary=`$scriptDir/build_taoskeeper.sh -r $arch -e taoskeeper`

sudo rm -rf /opt/tdengine/*

if [ -d "/usr/local/Cellar/tdengine/$version" ];then
  sudo cp -rf /usr/local/Cellar/tdengine/$version/ /opt/tdengine/
elif [ -d "/opt/homebrew/Cellar/tdengine/$version" ];then
  sudo cp -rf /opt/homebrew/Cellar/tdengine/$version/ /opt/tdengine/
else
  sudo cp -rf /usr/local/taos/ /opt/tdengine/
fi

sudo rm -rf /opt/tdengine/data
sudo rm -rf /opt/tdengine/log
sudo mkdir -p /opt/tdengine/service
sudo cp $communityDir/packaging/tools/{logo.png,TDengine,com.taosdata.*} /opt/tdengine/service/

sudo mkdir -p /opt/tdengine/examples/taosbenchmark-json
sudo cp $communityDir/tools/taos-tools/example/* /opt/tdengine/examples/taosbenchmark-json

sudo cp -f $taoskeeper_binary /opt/tdengine/bin/
sudo cp -f $(dirname $taoskeeper_binary)/taoskeeper.service /opt/tdengine/cfg/
sudo cp -f $(dirname $taoskeeper_binary)/config/taoskeeper.toml /opt/tdengine/cfg/
sudo chmod ugo+w /opt/tdengine/bin/remove.sh
sudo cat $scriptDir/remove_taoskeeper.sh >> /opt/tdengine/bin/remove.sh

cd $communityDir/packaging/tools
sed -i '' "s/TDengine-.*-macOS-.*\</TDengine-server-$version-macOS-$cpuType\</g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' "s/3.0.1.4/$version/g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' "s|/opt.*/tools/post.sh|$communityDir/packaging/tools/post.sh|g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' "s|/opt.*/tools/mac_before_install.txt|$communityDir/packaging/tools/mac_before_install.txt|g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' "s|/opt/.*/release|$topDir/release|g" $communityDir/packaging/tools/TDengine.pkgproj

/usr/local/bin/packagesbuild --package-version $version TDengine.pkgproj

sudo rm -rf /opt/tdengine/{service,bin/taosd,bin/udfd}
sed -i '' "s/TDengine-.*-macOS-.*\</TDengine-client-$version-macOS-$cpuType\</g" $communityDir/packaging/tools/TDengine.pkgproj
sed -i '' "s/mac_before_install.txt/mac_before_install_client.txt/g" $communityDir/packaging/tools/TDengine.pkgproj
/usr/local/bin/packagesbuild --package-version $version TDengine.pkgproj

cd $topDir/release
scp *client* root@taosdata.com:/data/www/assets-download/3.0/
if [ $? > 0 ]; then
  echo "copy client package to taosdata server failed"
fi
scp *client* ubuntu@tdengine.com:/data/www/assets-download/3.0/
if [ $? > 0 ]; then
  echo "copy client package to TDengine server failed"
fi