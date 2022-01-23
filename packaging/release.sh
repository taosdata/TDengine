#!/bin/bash
#
# Generate the deb package for ubuntu, or rpm package for centos, or tar.gz package for other linux os

set -e
#set -x

# set parameters by default value
version="3.0.0.0"

curr_dir=$(pwd)

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/..)"

echo "=======================new version number: ${verNumber}======================================"

build_time=$(date +"%F %R")

echo "top_dir: ${top_dir}"

cd ${top_dir}
git pull || :

echo "curr_dir: ${curr_dir}"

# 2. cmake executable file
compile_dir="${top_dir}/debug"
if [ -d ${compile_dir} ]; then
  rm -rf ${compile_dir}
fi

mkdir -p ${compile_dir}

cd ${compile_dir}

echo "compile_dir: ${compile_dir}"

cmake ..
make -j32

release_dir="${top_dir}/release"
if [ -d ${release_dir} ]; then
  rm -rf ${release_dir}
fi

mkdir -p ${release_dir}
cd ${release_dir}

install_dir="${release_dir}/TDengine-server-${version}"
mkdir -p ${install_dir}
mkdir -p ${install_dir}/lib

bin_files="${compile_dir}/source/dnode/mgmt/daemon/taosd ${compile_dir}/tools/shell/taos  ${compile_dir}/tests/test/c/create_table"
cp ${bin_files} ${install_dir}/ && chmod a+x ${install_dir}/* || :


cp ${compile_dir}/source/client/libtaos.so ${install_dir}/lib/
cp ${compile_dir}/source/dnode/mnode/impl/libmnode.so ${install_dir}/lib/
cp ${compile_dir}/source/dnode/qnode/libqnode.so ${install_dir}/lib/
cp ${compile_dir}/source/dnode/snode/libsnode.so ${install_dir}/lib/
cp ${compile_dir}/source/dnode/bnode/libbnode.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/wal/libwal.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/scheduler/libscheduler.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/planner/libplanner.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/parser/libparser.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/qcom/libqcom.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/transport/libtransport.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/function/libfunction.so ${install_dir}/lib/
cp ${compile_dir}/source/common/libcommon.so ${install_dir}/lib/
cp ${compile_dir}/source/os/libos.so ${install_dir}/lib/
cp ${compile_dir}/source/dnode/mnode/sdb/libsdb.so ${install_dir}/lib/
cp ${compile_dir}/source/libs/catalog/libcatalog.so ${install_dir}/lib/

pkg_name=${install_dir}-Linux-x64

tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files || :


