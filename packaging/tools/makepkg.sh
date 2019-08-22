#!/bin/bash
#
# Generate deb package for other os system (no unbutu or centos)

curr_dir=$(pwd)
compile_dir=$1
version=$2
build_time=$3

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -m ${script_dir}/../..)"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

package_name='linux'
install_dir="${release_dir}/taos-${version}-${package_name}-$(echo ${build_time}| tr ': ' -)"

# Directories and files.
bin_files="${build_dir}/bin/taosd ${build_dir}/bin/taos ${build_dir}/bin/taosdemo ${build_dir}/bin/taosdump ${script_dir}/remove.sh"
versioninfo=$(${script_dir}/get_version.sh)
lib_files="${build_dir}/lib/libtaos.so.${versioninfo}"
header_files="${code_dir}/inc/taos.h"
cfg_files="${top_dir}/packaging/cfg/*.cfg"
install_files="${script_dir}/install.sh ${script_dir}/install_client.sh"

# Init file
#init_dir=${script_dir}/deb
#if [ $package_type = "centos" ]; then
#    init_dir=${script_dir}/rpm
#fi
#init_files=${init_dir}/taosd
# temp use rpm's taosd. TODO: later modify according to os type
init_files=${script_dir}/../rpm/taosd

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_files} ${install_dir}/cfg
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/*
mkdir -p ${install_dir}/init.d    && cp ${init_files} ${install_dir}/init.d

cd ${install_dir}
tar -zcv -f taos.tar.gz * --remove-files || : 

cd ${curr_dir}
cp ${install_files} ${install_dir} && chmod a+x ${install_dir}/install*

# Copy example code
mkdir -p ${install_dir}/examples
cp -r ${top_dir}/tests/examples/c      ${install_dir}/examples
cp -r ${top_dir}/tests/examples/JDBC   ${install_dir}/examples
cp -r ${top_dir}/tests/examples/matlab ${install_dir}/examples
cp -r ${top_dir}/tests/examples/python ${install_dir}/examples
cp -r ${top_dir}/tests/examples/R      ${install_dir}/examples
cp -r ${top_dir}/tests/examples/go     ${install_dir}/examples

# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector
cp -r ${connector_dir}/grafana ${install_dir}/connector/
cp -r ${connector_dir}/python  ${install_dir}/connector/
cp -r ${connector_dir}/go      ${install_dir}/connector
cp ${build_dir}/lib/*.jar      ${install_dir}/connector


# Copy release note
cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir}  
tar -zcv -f "$(basename ${install_dir}).tar.gz" $(basename ${install_dir}) --remove-files

cd ${curr_dir}
