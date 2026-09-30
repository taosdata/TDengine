#!/bin/bash

# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -e
#set -x
set -v

# dockerbuild.sh
#             -n [version number]
#             -p [xxxx]
#             -V [stable | beta]

# set parameters by default value
version=""
passWord=""
verType=""
dockerLatest="n"

while getopts "hn:p:V:a:" arg
do
  case $arg in
    n)
      #echo "version=$OPTARG"
      version=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    V)
      #echo "verType=$OPTARG"
      verType=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -n [version number] "
      echo "                     -p [password for docker hub] "
      echo "                     -V [stable |beta] "
      echo "                     -a [y | n ]   "
      exit 0
      ;;
    a)
      #echo "dockerLatest=$OPTARG"
      dockerLatest=$(echo $OPTARG)
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

echo "version=${version}"

if [ "$verType" == "stable" ]; then
  verType=stable
dockerinput=TDengine-server-${version}-Linux-$cpuType.tar.xz
dockerinput_x64=TDengine-server-${version}-Linux-amd64.tar.xz
  dockerim=tdengine/tdengine
  dockeramd64=tdengine/tdengine-amd64
  dockeraarch64=tdengine/tdengine-aarch64
  dockeraarch32=tdengine/tdengine-aarch32
elif [ "$verType" == "beta" ];then
  verType=beta
  tagVal=ver-${version}-beta
dockerinput=TDengine-server-${version}-${verType}-Linux-$cpuType.tar.xz
dockerinput_x64=TDengine-server-${version}-${verType}-Linux-amd64.tar.xz
  dockerim=tdengine/tdengine-beta
  dockeramd64=tdengine/tdengine-amd64-beta
  dockeraarch64=tdengine/tdengine-aarch64-beta
  dockeraarch32=tdengine/tdengine-aarch32-beta
 else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

username="tdengine"

# generate docker version
echo "generate ${dockerim}:${version}"
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker manifest rm ${dockerim}:${version}
docker manifest create -a ${dockerim}:${version} ${dockeramd64}:${version} ${dockeraarch64}:${version}
docker manifest inspect  ${dockerim}:${version}
docker login -u ${username} -p ${passWord}
docker manifest push ${dockerim}:${version}


# generate docker latest
echo "generate ${dockerim}:latest "

if  [ ${dockerLatest} == 'y' ]  ;then
    echo "docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest"
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker manifest rm ${dockerim}:latest
    docker manifest create -a ${dockerim}:latest ${dockeramd64}:latest ${dockeraarch64}:latest
    docker manifest inspect  ${dockerim}:latest
    docker login -u tdengine -p ${passWord}  #replace the docker registry username and password
    docker manifest push ${dockerim}:latest
    docker pull tdengine/tdengine:latest

fi

