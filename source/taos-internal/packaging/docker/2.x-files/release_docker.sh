#!/bin/bash

# **************************************************************
# Script to release TAOS install package.
# **************************************************************

set -e

currDir=$(pwd)
codeDir=$(readlink -m ${currDir}/../..)
rootDir=$(readlink -m ${codeDir}/..)
buildDir=$(readlink -m ${rootDir}/build)
releaseDir=$(readlink -m ${rootDir}/release)


# # --------------------Get version information
versionInfo="${codeDir}/util/src/version.c"
version=$(cat ${versionInfo} | grep version | cut -d '"' -f2)

if [ "$1" != "test" ]; then 
    while true; do
        read -p "Do you want to release a new version? [y/N]: " isVersionChange

        if [[ ( "${isVersionChange}" == "y") || ( "${isVersionChange}" == "Y") ]]; then
            # TODO: Add version format check here.
            read -p "Please enter the new version: " version
            break
        elif [[ ( "${isVersionChange}" == "n") || ( "${isVersionChange}" == "N") ]]; then
            echo "Use old version ${version}"
            break
        else
            continue
        fi
    done
fi

buildTime=$(date +"%F %R")
echo "char version[64] = \"${version}\";"                            > ${versionInfo}
echo "char buildinfo[512] = \"Built by ${USER} at ${buildTime}\";"  >> ${versionInfo}

# --------------------------Make executable file.
cd ${codeDir}
make clean
make
cd ${currDir}

# --------------------------Group files
# create compressed install file.
installDir="tdengine-docker-${version}-$(echo ${buildTime}| tr ': ' -)-${USER}"

# directories and files.
binDir="bin"
libDir="lib"
headerDir="inc"
cfgDir="cfg"

binFiles="${buildDir}/bin/tdengine ${buildDir}/bin/tdengined ${currDir}/remove.sh"
libFiles="${buildDir}/lib/libtaos.so ${buildDir}/lib/libtaos.a"
headerFiles="${codeDir}/inc/taos.h"
cfgFiles="${codeDir}/cfg/*"

dockerFiles="${currDir}/Dockerfile.tdengined ${currDir}/Dockerfile.tdengine"
installFiles="${currDir}/install.sh"

# make directories.
mkdir -p ${installDir}
mkdir -p ${installDir}/${binDir} && cp ${binFiles} ${installDir}/${binDir}
mkdir -p ${installDir}/${libDir} && cp ${libFiles} ${installDir}/${libDir}
mkdir -p ${installDir}/${headerDir} && cp ${headerFiles} ${installDir}/${headerDir}
mkdir -p ${installDir}/${cfgDir} && cp ${cfgFiles} ${installDir}/${cfgDir}
cp ${dockerFiles} ${installDir}

cp ${rootDir}/build/lib/JDBCDriver*-dist.* ${installDir} 2> /dev/null || :

cd ${installDir}
tar -zcf tdengine.tar.gz * --remove-files 
cd ${currDir}

cp ${installFiles} ${installDir}

# Copy example code
cp -r ${codeDir}/examples ${installDir}

tar -zcf "${installDir}.tar.gz" ${installDir} --remove-files

mkdir -p ${releaseDir}
mv "${installDir}.tar.gz" ${releaseDir}
