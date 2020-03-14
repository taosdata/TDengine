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
dbName=$9

if [ "$dbName" == "taos" ]; then
  DB_CLIENT_NAME="taos"
  DB_SERVICE_NAME="taosd"
  DB_FULL_NAME="TDengine"
  DB_COPYRIGHT="TAOS Data"
  DB_COMPANY="taosdata" 
elif [ "$dbName" == "power" ]; then
  DB_CLIENT_NAME="power"
  DB_SERVICE_NAME="powerd"
  DB_FULL_NAME="PowerDB"
  DB_COPYRIGHT="PowerDB"
  DB_COMPANY="powerdb"   
else
  echo "Not support dbname, exit"
  exit 1
fi

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
    install_dir="${release_dir}/${DB_FULL_NAME}-enterprise-client"
else
    install_dir="${release_dir}/${DB_FULL_NAME}-client"
fi

# Directories and files.

if [ "$osType" != "Darwin" ]; then
  if [ "$pagMode" == "lite" ]; then
    strip ${build_dir}/bin/${DB_SERVICE_NAME} 
    strip ${build_dir}/bin/${DB_CLIENT_NAME}
    bin_files="${build_dir}/bin/${DB_CLIENT_NAME} ${script_dir}/remove_client.sh"
  else  
    bin_files="${build_dir}/bin/${DB_CLIENT_NAME} ${build_dir}/bin/${DB_CLIENT_NAME}dump ${script_dir}/remove_client.sh"
  fi
  
  if [ "$verMode" == "cluster" ]; then
    lib_files="${build_dir}/lib/libtaos.so.${version} ${build_dir}/lib/libtaosodbc.so"
  else
    lib_files="${build_dir}/lib/libtaos.so.${version}"
  fi  
else
  bin_files="${build_dir}/bin/${DB_CLIENT_NAME} ${script_dir}/remove_client.sh"
  lib_files="${build_dir}/lib/libtaos.${version}.dylib"
fi

header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
cfg_dir="${top_dir}/packaging/cfg"

install_files="${script_dir}/install_client.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/taos.cfg ${install_dir}/cfg/${DB_CLIENT_NAME}.cfg
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/*

sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/bin/remove_client.sh
sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/bin/remove_client.sh
sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/bin/remove_client.sh

cd ${install_dir}

if [ "$osType" != "Darwin" ]; then
    tar -zcv -f ${DB_CLIENT_NAME}.tar.gz * --remove-files || :
else
    tar -zcv -f ${DB_CLIENT_NAME}.tar.gz * || :
    mv ${DB_CLIENT_NAME}.tar.gz ..
    rm -rf ./*
    mv ../${DB_CLIENT_NAME}.tar.gz .
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$osType" == "Darwin" ]; then
    sed 's/osType=Linux/osType=Darwin/g' ${install_dir}/install_client.sh >> install_client_temp.sh
    mv install_client_temp.sh ${install_dir}/install_client.sh
fi
if [ "$pagMode" == "lite" ]; then
    sed 's/pagMode=full/pagMode=lite/g' ${install_dir}/install_client.sh >> install_client_temp.sh
    mv install_client_temp.sh ${install_dir}/install_client.sh
fi

if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/install_client.sh >> install_client_temp.sh
    mv install_client_temp.sh ${install_dir}/install_client.sh
fi

sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/install_client.sh
sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/install_client.sh
sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/install_client.sh

chmod a+x ${install_dir}/install_client.sh

# Copy example code
mkdir -p ${install_dir}/examples
examples_dir="${top_dir}/tests/examples"
cp -r ${examples_dir}/c      ${install_dir}/examples
if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  cp -r ${examples_dir}/JDBC   ${install_dir}/examples
  cp -r ${examples_dir}/matlab ${install_dir}/examples
  cp -r ${examples_dir}/python ${install_dir}/examples
  cp -r ${examples_dir}/R      ${install_dir}/examples
  cp -r ${examples_dir}/go     ${install_dir}/examples
fi
# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector

if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  if [ "$osType" != "Darwin" ]; then
    cp ${build_dir}/lib/*.jar      ${install_dir}/connector
  fi
  cp -r ${connector_dir}/grafana ${install_dir}/connector/
  cp -r ${connector_dir}/python  ${install_dir}/connector/
  cp -r ${connector_dir}/go      ${install_dir}/connector
fi
# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir} 

if [ "$verMode" == "cluster" ]; then
  pkg_name=${install_dir}-${version}-${osType}-${cpuType}
elif [ "$verMode" == "edge" ]; then
  pkg_name=${install_dir}-${version}-${osType}-${cpuType}
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
