#!/bin/bash

set -e

# new_ver_release.sh  -b [develop | master] 
#                     -c [aarch32 | aarch64 | x64 ...]  
#                     -n [2.1.*.* | 2.0.*.* ]
#                     -l [full | lite] 
#                     -v [cluster, edge ,all] cluster is enterprise, edge is community
#                     -d [isdocker ] 
#                     -h help

# set parameters by default value
branchName=master   # -b [develop | master
cpuType=x64         # -c [aarch32 | aarch64 | x64 ...]
version="2.1.4.1"   # -n [2.1.*.* | 2.0.*.* ]
pagMode=full        # -l [full | lite] 
verMode=all        # -v [cluster, edge ,all ] cluster is enterprise, edge is community
versionComp=2.0.0.0
dockerMode=""
preEnter="no"

while getopts "hb:c:n:l:v:d:p:" arg
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
    d)
      #echo "dockerMode=$OPTARG"
      dockerMode=$(echo $OPTARG)
      ;;
    p)
      #echo "preEnter=$OPTARG"
      preEnter=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -b [develop | master] "
      echo "                          -c [aarch32 | aarch64 | x64 ...] "
      echo "                          -n [version number: 2.1.*.* | 2.0.*.* ]      "
      echo "                          -l [full | lite]  "
      echo "                          -v [cluster, edge ,all] cluster is enterprise, edge is community  "
      echo "                          -d [isdocker | other ]   "
      echo "                          -p [pre | other ]   "
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done



# new workPath
workPath=/home/ubuntu/workroom/jenkins/
if [ ! -d $workPath ]; then
    mkdir -p $workPath
fi

#reposisitory path
repPath=$workPath/TDinternal
communityDir=$workPath/TDinternal/community
releaseBranch=release/ver-${version}

# new workdir
if [ ! -d $repPath ]; then
    cd ${workPath} && git clone git@github.com:taosdata/TDinternal.git --recursive --recurse-submodules
else
    rm -rf $repPath
    echo " delete latest $repPath "
    sleep 10
    cd ${workPath}  &&  git clone git@github.com:taosdata/TDinternal.git --recursive --recurse-submodules
fi


# new branch

cd ${repPath}
if git rev-parse --verify remotes/origin/release/ver-${version} ; then
    git checkout -f ${branchName} && git pull origin ${branchName} --no-edit 
    git fetch && git checkout  ${releaseBranch}  && git pull origin ${releaseBranch} --no-edit 
else
    git checkout -f ${branchName} && git pull origin ${branchName} --no-edit 
    git fetch && git checkout -b ${releaseBranch} 
    sed -i "7s/.*TD_VER_NUMBER.*/  SET(TD_VER_NUMBER \""$version"\")/"  ${repPath}/community/cmake/version.inc
    sed -i "3s/version.*/version: \'"$version"\'/"  ${repPath}/community/snap/snapcraft.yaml
    sed -i "75s/.*libtaos.so.*/      - usr\/lib\/libtaos.so.$version/"   ${repPath}/community/snap/snapcraft.yaml
    git push --set-upstream origin ${releaseBranch}
fi

cd ${communityDir}
if git rev-parse --verify remotes/origin/release/ver-${version} ; then
    git checkout -f ${branchName} && git pull origin ${branchName} --no-edit 
    git fetch && git checkout  ${releaseBranch}  && git pull origin ${releaseBranch} --no-edit 
else
    git checkout -f ${branchName} && git pull origin ${branchName} --no-edit 
    git fetch && git checkout -b ${releaseBranch} 
    sed -i "7s/.*TD_VER_NUMBER.*/  SET(TD_VER_NUMBER \""$version"\")/" ${repPath}/community/cmake/version.inc
    sed -i "3s/version.*/version: \'"$version"\'/"  ${repPath}/community/snap/snapcraft.yaml
    sed -i "75s/.*libtaos.so.*/      - usr\/lib\/libtaos.so.$version/"   ${repPath}/community/snap/snapcraft.yaml

    git push --set-upstream origin ${releaseBranch}
fi

#  packaging x64/aarch32/aarch64 


if [ "${preEnter}" != "pre" ];then
    cd ${repPath}/enterprise/packaging
    ./new_ver_release.sh -b ${branchName} -c ${cpuType} -n ${version} -v ${verMode} -d ${dockerMode}
elif [ "${preEnter}" == "pre" ];then
    cd ${repPath}/enterprise/packaging
    ./new_ver_release_pre.sh -b ${branchName} -c ${cpuType} -n ${version} -v ${verMode} -d ${dockerMode}
fi




# # manifest docker 
# cd ${communityDir}/packaging/docker 

# if [ ${branchName} == "master" ];then
#     ./dockerManifest.sh -n ${version} -p tbase125! -V stable
# elif [ ${branchName} == "develop" ];then
#     ./dockerManifest.sh -n ${version} -p tbase125! -V beta
# fi

