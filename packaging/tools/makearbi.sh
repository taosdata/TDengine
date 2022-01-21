#!/bin/bash
#
# Generate arbitrator's tar.gz setup package for all os system

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

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"

productName="TDengine"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'
if [ "$verMode" == "cluster" ]; then
  install_dir="${release_dir}/${productName}-enterprise-arbitrator-${version}"
else
  install_dir="${release_dir}/${productName}-arbitrator-${version}"
fi

# Directories and files.
bin_files="${build_dir}/bin/tarbitrator ${script_dir}/remove_arbi.sh"
install_files="${script_dir}/install_arbi.sh"

#header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taosdef.h ${code_dir}/inc/taoserror.h"
init_file_tarbitrator_deb=${script_dir}/../deb/tarbitratord
init_file_tarbitrator_rpm=${script_dir}/../rpm/tarbitratord

# make directories.
mkdir -p ${install_dir} && cp ${install_files} ${install_dir} && chmod a+x ${install_dir}/install_arbi.sh || :
#mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc || :
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/* || :
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_deb} ${install_dir}/init.d/tarbitratord.deb || :
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_rpm} ${install_dir}/init.d/tarbitratord.rpm || :

cd ${release_dir}

#  install_dir has been distinguishes  cluster from  edege, so comments this code
pkg_name=${install_dir}-${osType}-${cpuType}

if [[ "$verType" == "beta" ]] || [[ "$verType" == "preRelease" ]]; then
  pkg_name=${install_dir}-${verType}-${osType}-${cpuType}
elif [ "$verType" == "stable" ]; then
  pkg_name=${pkg_name}
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files || :
exitcode=$?
if [ "$exitcode" != "0" ]; then
  echo "tar ${pkg_name}.tar.gz error !!!"
  exit $exitcode
fi

cd ${curr_dir}
