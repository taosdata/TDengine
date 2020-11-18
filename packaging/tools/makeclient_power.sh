#!/bin/bash
#
# Generate tar.gz package for linux client in all os system
set -e
#set -x

curr_dir=$(pwd)
compile_dir=$1
version=$2
build_time=$3
cpuType=$4
osType=$5
verMode=$6
verType=$7
pagMode=$8

if [ "$osType" != "Darwin" ]; then
    script_dir="$(dirname $(readlink -f $0))"
    top_dir="$(readlink -f ${script_dir}/../..)"
else
    script_dir=`dirname $0`
    cd ${script_dir}
    script_dir="$(pwd)"
    top_dir=${script_dir}/../..
fi

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'

if [ "$verMode" == "cluster" ]; then
    install_dir="${release_dir}/PowerDB-enterprise-client-${version}"
else
    install_dir="${release_dir}/PowerDB-client-${version}"
fi

# Directories and files.

if [ "$osType" != "Darwin" ]; then
#  if [ "$pagMode" == "lite" ]; then
#    strip ${build_dir}/bin/powerd 
#    strip ${build_dir}/bin/power
#    bin_files="${build_dir}/bin/power ${script_dir}/remove_client_power.sh"
#  else  
#    bin_files="${build_dir}/bin/power ${build_dir}/bin/powerdemo ${script_dir}/remove_client_power.sh ${script_dir}/set_core.sh"
#  fi
  lib_files="${build_dir}/lib/libtaos.so.${version}"
else
  bin_files="${build_dir}/bin/power ${script_dir}/remove_client_power.sh"
  lib_files="${build_dir}/lib/libtaos.${version}.dylib"
fi

header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
cfg_dir="${top_dir}/packaging/cfg"

install_files="${script_dir}/install_client_power.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/taos.cfg ${install_dir}/cfg/taos.cfg

sed -i '/dataDir/ {s/taos/power/g}'  ${install_dir}/cfg/taos.cfg
sed -i '/logDir/  {s/taos/power/g}'  ${install_dir}/cfg/taos.cfg
sed -i "s/TDengine/PowerDB/g"        ${install_dir}/cfg/taos.cfg

mkdir -p ${install_dir}/bin
if [ "$osType" != "Darwin" ]; then
  if [ "$pagMode" == "lite" ]; then
    strip ${build_dir}/bin/taos
    cp ${build_dir}/bin/taos          ${install_dir}/bin/power
    cp ${script_dir}/remove_power.sh  ${install_dir}/bin
  else 
    cp ${build_dir}/bin/taos          ${install_dir}/bin/power
    cp ${script_dir}/remove_power.sh  ${install_dir}/bin
    cp ${build_dir}/bin/taosdemo      ${install_dir}/bin/powerdemo 
    cp ${build_dir}/bin/taosdump      ${install_dir}/bin/powerdump  
    cp ${script_dir}/set_core.sh      ${install_dir}/bin
    cp ${script_dir}/get_client.sh    ${install_dir}/bin
  fi
else
  cp ${bin_files} ${install_dir}/bin
fi
chmod a+x ${install_dir}/bin/* || :

cd ${install_dir}

if [ "$osType" != "Darwin" ]; then
    tar -zcv -f power.tar.gz * --remove-files || :
else
    tar -zcv -f power.tar.gz * || :
    mv power.tar.gz ..
    rm -rf ./*
    mv ../power.tar.gz .
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$osType" == "Darwin" ]; then
    sed 's/osType=Linux/osType=Darwin/g' ${install_dir}/install_client_power.sh >> install_client_power_temp.sh
    mv install_client_power_temp.sh ${install_dir}/install_client_power.sh
fi
if [ "$pagMode" == "lite" ]; then
    sed 's/pagMode=full/pagMode=lite/g' ${install_dir}/install_client_power.sh >> install_client_power_temp.sh
    mv install_client_power_temp.sh ${install_dir}/install_client_power.sh
fi
chmod a+x ${install_dir}/install_client_power.sh

# Copy example code
mkdir -p ${install_dir}/examples
examples_dir="${top_dir}/tests/examples"
cp -r ${examples_dir}/c      ${install_dir}/examples
sed -i '/passwd/ {s/taosdata/powerdb/g}'  ${install_dir}/examples/c/*.c
sed -i '/root/   {s/taosdata/powerdb/g}'  ${install_dir}/examples/c/*.c

if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  cp -r ${examples_dir}/JDBC   ${install_dir}/examples
  cp -r ${examples_dir}/matlab ${install_dir}/examples
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/examples/matlab/TDengineDemo.m
  cp -r ${examples_dir}/python ${install_dir}/examples  
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/examples/python/read_example.py
  cp -r ${examples_dir}/R      ${install_dir}/examples
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/examples/R/command.txt
  cp -r ${examples_dir}/go     ${install_dir}/examples  
  sed -i '/root/ {s/taosdata/powerdb/g}'  ${install_dir}/examples/go/taosdemo.go
fi
# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector

if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  if [ "$osType" != "Darwin" ]; then
    cp ${build_dir}/lib/*.jar      ${install_dir}/connector ||:
  fi
  cp -r ${connector_dir}/grafanaplugin ${install_dir}/connector/
  cp -r ${connector_dir}/python  ${install_dir}/connector/
  cp -r ${connector_dir}/go      ${install_dir}/connector 
  
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python2/taos/cinterface.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python3/taos/cinterface.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python2/taos/cinterface.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python3/taos/cinterface.py
  
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python2/taos/subscription.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python3/taos/subscription.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python2/taos/subscription.py
  sed -i '/password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python3/taos/subscription.py
  
  sed -i '/self._password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python2/taos/connection.py
  sed -i '/self._password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/linux/python3/taos/connection.py
  sed -i '/self._password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python2/taos/connection.py
  sed -i '/self._password/ {s/taosdata/powerdb/g}'  ${install_dir}/connector/python/windows/python3/taos/connection.py 
fi
# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir} 

if [ "$verMode" == "cluster" ]; then
  pkg_name=${install_dir}-${osType}-${cpuType}
elif [ "$verMode" == "edge" ]; then
  pkg_name=${install_dir}-${osType}-${cpuType}
else
  echo "unknow verMode, nor cluster or edge"
  exit 1
fi

if [ "$pagMode" == "lite" ]; then
  pkg_name=${pkg_name}-Lite
fi

if [ "$verType" == "beta" ]; then
  pkg_name=${pkg_name}-${verType}
elif [ "$verType" == "stable" ]; then  
  pkg_name=${pkg_name} 
else
  echo "unknow verType, nor stable or beta"
  exit 1
fi

if [ "$osType" != "Darwin" ]; then
    tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files || :
else
    tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) || :
    mv "$(basename ${pkg_name}).tar.gz" ..
    rm -rf ./*
    mv ../"$(basename ${pkg_name}).tar.gz" .
fi

cd ${curr_dir}
