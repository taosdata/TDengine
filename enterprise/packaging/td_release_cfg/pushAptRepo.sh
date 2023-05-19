
#!/bin/bash

set -e

# new_ver_release.sh  -b [develop | master | 2.0 | 3.0]
#                     -c [aarch32 | aarch64 | x64 ...] "
#                     -n [2.1.*.* | 2.0.*.* ]
#                     -h help

# set parameters by default value
branchName=develop   # -b [develop | master | 2.0 | 3.0]
cpuType=x64         # -c [aarch32 | aarch64 | x64 ...]
version="3.0.0.0"   # -n [2.1.*.* | 2.0.*.* ]


while getopts "hb:c:n:" arg
do
  case $arg in
    b)
      #echo "branchName=$OPTARG"
      branchName=$(echo $OPTARG)
      ;;
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master | 2.0 | 3.0]"
      echo "                          -c [aarch32 | aarch64 | x64 ...] "
      echo "                          -n [version number: 2.1.*.* | 2.0.*.* ]      "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done



#define path and variables
workPath=/var/www/html/
if [ "$branchName" == "3.0" ] || [ "$branchName" == "2.0" ] || [ "$branchName" == "main" ];then
  verType=stable
  repPath=${workPath}/tdengine-${verType}/
  packagePath=${repPath}/pool/main/
  debinput=TDengine-server-${version}-Linux-${cpuType}.deb
  debinput_amd64=TDengine-server-${version}-Linux-amd64.deb
elif [ "$branchName" == "develop" ];then
  verType=beta
  repPath=${workPath}/tdengine-${verType}/
  packagePath=${repPath}/pool/main/
#  debinput=TDengine-server-${version}-${verType}-Linux-$cpuType.deb
  debinput=TDengine-server-${version}-Linux-$cpuType.deb
#  debinput_amd64=TDengine-server-${version}-${verType}-Linux-amd64.deb
  debinput_amd64=TDengine-server-${version}-Linux-amd64.deb
fi

distsPath=${repPath}/dists/${verType}/main/binary-amd64
releasePath=${repPath}/dists/${verType}/
echo -e " repPath=${repPath}\n packagePath=${packagePath}\n distsPath=${distsPath}\n releasePath=${releasePath}\n debinput=${debinput}\n debinput_amd64=${debinput_amd64} "

# copy package to repo
sshpass -p taosdata125! scp root@192.168.1.131:/nas/TDengine/v${version}/community/${debinput}  ${packagePath}/${debinput_amd64}

echo "copy package ${debinput} to repos.taosdata.com successfully"

#push package to apt repo
cd ${repPath}
dpkg-scanpackages -m pool | tee ${distsPath}/Packages
cat ${distsPath}/Packages | gzip -9 | tee ${distsPath}/Packages.gz
cd ${releasePath} && /root/generate-release-${verType}.sh > Release

export GNUPGHOME=/home/ubuntu/pgpkeys
cd ${repPath}
cat ${releasePath}/Release | gpg --default-key TDengine -abs | sudo tee ${releasePath}/Release.gpg



