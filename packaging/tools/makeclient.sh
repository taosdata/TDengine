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

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'
install_dir="${release_dir}/TDengine-client"

# Directories and files.
bin_files="${build_dir}/bin/taos ${build_dir}/bin/taosdump ${script_dir}/remove_client.sh"
lib_files="${build_dir}/lib/libtaos.so.${version}"
header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
cfg_dir="${top_dir}/packaging/cfg"
install_files="${script_dir}/install_client.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/taos.cfg ${install_dir}/cfg/taos.cfg
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/*

cd ${install_dir}
tar -zcv -f taos.tar.gz * --remove-files || :

cd ${curr_dir}
cp ${install_files} ${install_dir} && chmod a+x ${install_dir}/install*

# Copy example code
mkdir -p ${install_dir}/examples
examples_dir="${top_dir}/tests/examples"
cp -r ${examples_dir}/c      ${install_dir}/examples
cp -r ${examples_dir}/JDBC   ${install_dir}/examples
cp -r ${examples_dir}/matlab ${install_dir}/examples
cp -r ${examples_dir}/python ${install_dir}/examples
cp -r ${examples_dir}/R      ${install_dir}/examples
cp -r ${examples_dir}/go     ${install_dir}/examples

# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector
cp ${build_dir}/lib/*.jar      ${install_dir}/connector
cp -r ${connector_dir}/grafana ${install_dir}/connector/
cp -r ${connector_dir}/python  ${install_dir}/connector/
cp -r ${connector_dir}/go      ${install_dir}/connector

# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir} 

if [ "$verMode" == "cluster" ]; then
  pkg_name=${install_dir}-${version}-${osType}-${cpuType}
elif [ "$verMode" == "lite" ]; then
  pkg_name=${install_dir}-${version}-${osType}-${cpuType}
else
  echo "unknow verMode, nor cluster or lite"
  exit 1
fi

if [ "$verType" == "beta" ]; then
  pkg_name=${pkg_name}-${verType}
elif [ "$verType" == "stable" ]; then  
  pkg_name=${pkg_name} 
else
  echo "unknow verType, nor stable or beta"
  exit 1
fi

tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files

cd ${curr_dir}
