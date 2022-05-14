#!/bin/bash
#
# Generate the tar.gz package for linux os

set -e
#set -x

# set parameters by default value
version="3.0.0.0"

curr_dir=$(pwd)

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/..)"

echo "=======================new version number: ${verNumber}======================================"

build_time=$(date +"%F %R")

echo "script_dir: ${script_dir}"
echo "top_dir: ${top_dir}"

cd ${top_dir}
# git checkout -- .
# git checkout 3.0
# git pull || :

echo "curr_dir: ${curr_dir}"

# 2. cmake executable file
compile_dir="${top_dir}/debug"
# if [ -d ${compile_dir} ]; then
#   rm -rf ${compile_dir}
# fi

mkdir -p ${compile_dir}

cd ${compile_dir}

echo "compile_dir: ${compile_dir}"

cmake .. -DBUILD_TOOLS=true
make -j32

release_dir="${top_dir}/release"
if [ -d ${release_dir} ]; then
  rm -rf ${release_dir}
fi

mkdir -p ${release_dir}
cd ${release_dir}

install_dir="${release_dir}/TDengine-server-${version}"
mkdir -p ${install_dir}
mkdir -p ${install_dir}/bin
mkdir -p ${install_dir}/lib
mkdir -p ${install_dir}/inc

install_files="${script_dir}/tools/install.sh"
chmod a+x ${script_dir}/tools/install.sh || :
cp ${install_files} ${install_dir} 

header_files="${top_dir}/include/client/taos.h ${top_dir}/include/util/taoserror.h"
cp ${header_files} ${install_dir}/inc
 
bin_files="${compile_dir}/build/bin/taosd ${compile_dir}/build/bin/taos  ${compile_dir}/build/bin/create_table ${compile_dir}/build/bin/tmq_sim ${script_dir}/tools/remove.sh ${compile_dir}/build/bin/taosBenchmark   ${compile_dir}/build/bin/taosdump"
cp -rf ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/* || :

cp ${compile_dir}/build/lib/libtaos.so  ${install_dir}/lib/
cp ${compile_dir}/build/lib/libavro* ${install_dir}/lib/ > /dev/null || echo -e "failed to copy avro libraries"
cp -rf ${compile_dir}/build/lib/pkgconfig  ${install_dir}/lib/ > /dev/null || echo -e "failed to copy pkgconfig directory"


#cp ${compile_dir}/source/dnode/mnode/impl/libmnode.so ${install_dir}/lib/
#cp ${compile_dir}/source/dnode/qnode/libqnode.so ${install_dir}/lib/
#cp ${compile_dir}/source/dnode/snode/libsnode.so ${install_dir}/lib/
#cp ${compile_dir}/source/dnode/bnode/libbnode.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/wal/libwal.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/scheduler/libscheduler.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/planner/libplanner.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/parser/libparser.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/qcom/libqcom.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/transport/libtransport.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/function/libfunction.so ${install_dir}/lib/
#cp ${compile_dir}/source/common/libcommon.so ${install_dir}/lib/
#cp ${compile_dir}/source/os/libos.so ${install_dir}/lib/
#cp ${compile_dir}/source/dnode/mnode/sdb/libsdb.so ${install_dir}/lib/
#cp ${compile_dir}/source/libs/catalog/libcatalog.so ${install_dir}/lib/

pkg_name=${install_dir}-Linux-x64

tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files || :


