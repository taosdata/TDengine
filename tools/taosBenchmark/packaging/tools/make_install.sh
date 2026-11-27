#!/bin/bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

osType=`uname`

dumpName="taosdump"
benchmarkName="taosBenchmark"
demoName="taosdemo"

source_dir=$1

if [ "$osType" != "Darwin" ]; then
  installDir="/usr/local/taos"
else
  if [ -d $source_dir/build/lib ]; then
    verNumber=`ls $source_dir/build/lib | grep -E "libtaos\.[0-9]\.[0-9]" | sed "s/libtaos.//g" |  sed "s/.dylib//g" | head -n 1`
  else
    verNumber=`taos -V|awk '{print $2}'|sed -e 's/\r//'`
  fi
  if [ -d "/usr/local/Cellar/" ];then
    installDir="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ];then
    installDir="/opt/homebrew/Cellar/tdengine/${verNumber}"
  else
    installDir="/usr/local/taos"
  fi
fi

csudo=""

if command -v sudo > /dev/null; then
    csudo="sudo "
fi

[ ! -d ${installDir}/bin ] && ${csudo}mkdir -p ${installDir}/bin
if [ "$osType" != "Darwin" ]; then
    [ -f ${source_dir}/build/bin/${dumpName} ] && ${csudo}cp ${source_dir}/build/bin/${dumpName} ${installDir}/bin ||:
    [ -f ${installDir}/bin/${dumpName} ] && ${csudo}ln -sf ${installDir}/bin/${dumpName} /usr/local/bin/${dumpName} ||:
fi
[ -f ${source_dir}/build/bin/${benchmarkName} ] && ${csudo}cp ${source_dir}/build/bin/${benchmarkName} ${installDir}/bin ||:
[ -f ${installDir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${benchmarkName} ||:
[ -f ${installDir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${demoName} ||:

#if [ -f ${source_dir}/build/lib/libavro.so.23.0.0 ]; then
#    ${csudo}cp -rf ${source_dir}/build/lib/libavro* /usr/local/lib > /dev/null || echo -e "failed to copy avro libraries"
#    ${csudo}cp -rf ${source_dir}/build/lib/pkgconfig /usr/local/lib > /dev/null || echo -e "failed to copy pkgconfig directory"
#fi

#if [ -f ${source_dir}/build/lib64/libavro.so.23.0.0 ]; then
#    ${csudo}cp -rf ${source_dir}/build/lib64/libavro* /usr/local/lib > /dev/null || echo -e "failed to copy avro libraries"
#    ${csudo}cp -rf ${source_dir}/build/lib64/pkgconfig /usr/local/lib > /dev/null || echo -e "failed to copy pkgconfig directory"
#fi

#if [ -d /etc/ld.so.conf.d ]; then
#    echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/libavro.conf > /dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
#    ${csudo}ldconfig || echo -e "failed to run ldconfig"
#else
#    echo "/etc/ld.so.conf.d not found!"
#fi
