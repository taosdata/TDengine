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

# dockerbuild.sh 
#             -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...]
#             -n [version number]
#             -p [password for docker hub]

# set parameters by default value
cpuType=aarch64
verNumber=""
passWord=""

while getopts "hc:n:p:f:" arg
do
  case $arg in
    c)
      #echo "cpuType=$OPTARG"
      cpuType=$(echo $OPTARG)
      ;;
    n)
      #echo "verNumber=$OPTARG"
      verNumber=$(echo $OPTARG)
      ;;
    p)
      #echo "passWord=$OPTARG"
      passWord=$(echo $OPTARG)
      ;;
    h)
      echo "Usage: `basename $0` -c [aarch32 | aarch64 | amd64 | x86 | mips64 | loongarch64...] "
      echo "                      -n [version number] "
      echo "                      -p [password for docker hub] "
      exit 0
      ;;
    ?) #unknow option 
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

pkgFile=TDengine-server-${verNumber}-Linux-${cpuType}.tar.gz

echo "cpuType=${cpuType} verNumber=${verNumber} pkgFile=${pkgFile} "

scriptDir=`pwd`
pkgDir=$scriptDir/../../release/

cp -f ${pkgDir}/${pkgFile} .

./dockerbuild.sh -c ${cpuType} -f ${pkgFile} -n ${verNumber} -p ${passWord}

rm -f ${pkgFile}
