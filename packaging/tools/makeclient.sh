#!/bin/bash
#
# Generate tar.gz package for linux client in all os system
set -e
set -x

curr_dir=$(pwd)
compile_dir=$1
version=$2
build_time=$3
cpuType=$4
osType=$5
verMode=$6
verType=$7
pagMode=$8
#comVersion=$9
dbName=$10

productName2="${11}"
#serverName2="${12}d"
clientName2="${12}"
# cusEmail2=${13}

productName="TDengine"
clientName="taos"
benchmarkName="taosBenchmark"
dumpName="taosdump"
configFile="taos.cfg"
tarName="package.tar.gz"

benchmarkName2="${clientName2}Benchmark"
dumpName2="${clientName2}dump"

if [ "$osType" != "Darwin" ]; then
  script_dir="$(dirname $(readlink -f $0))"
  top_dir="$(readlink -f ${script_dir}/../..)"
else
  script_dir=$(dirname $0)
  cd ${script_dir}
  script_dir="$(pwd)"
  top_dir=${script_dir}/../..
fi

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}"
release_dir="${top_dir}/release"

#package_name='linux'

if [ "$verMode" == "cluster" ]; then
  install_dir="${release_dir}/${productName2}-enterprise-client-${version}"
elif [ "$verMode" == "cloud" ]; then
  install_dir="${release_dir}/${productName2}-cloud-client-${version}"
else
  install_dir="${release_dir}/${productName2}-client-${version}"
fi

# Directories and files.

#if [ "$verMode" == "cluster" ]; then
#  sed -i 's/verMode=edge/verMode=cluster/g' ${script_dir}/remove_client.sh
#  sed -i "s/clientName2=\"taos\"/clientName2=\"${clientName2}\"/g" ${script_dir}/remove_client.sh
#  sed -i "s/configFile2=\"taos\"/configFile2=\"${clientName2}\"/g" ${script_dir}/remove_client.sh
#  sed -i "s/productName2=\"TDengine\"/productName2=\"${productName2}\"/g" ${script_dir}/remove_client.sh
#fi

if [ "$osType" != "Darwin" ]; then
  if [ "$pagMode" == "lite" ]; then
    strip ${build_dir}/bin/${clientName}
    bin_files="${build_dir}/bin/${clientName} \
        ${script_dir}/remove_client.sh"
  else
    bin_files="${build_dir}/bin/${clientName} \
        ${build_dir}/bin/${benchmarkName} \
        ${build_dir}/bin/${dumpName} \
        ${script_dir}/remove_client.sh \
        ${script_dir}/set_core.sh \
        ${script_dir}/get_client.sh"
  fi
  lib_files="${build_dir}/lib/libtaos.so.${version}"
  wslib_files="${build_dir}/lib/libtaosws.so"
else
  bin_files="${build_dir}/bin/${clientName} ${script_dir}/remove_client.sh"
  lib_files="${build_dir}/lib/libtaos.${version}.dylib"
  wslib_files="${build_dir}/lib/libtaosws.dylib"
fi

header_files="${code_dir}/include/client/taos.h ${code_dir}/include/common/taosdef.h ${code_dir}/include/util/taoserror.h ${code_dir}/include/util/tdef.h ${code_dir}/include/libs/function/taosudf.h"
wsheader_files="${build_dir}/include/taosws.h"

if [ "$dbName" != "taos" ]; then
  cfg_dir="${top_dir}/../enterprise/packaging/cfg"
else
  cfg_dir="${top_dir}/packaging/cfg"
fi

install_files="${script_dir}/install_client.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
[ -f ${wsheader_files} ] && cp ${wsheader_files} ${install_dir}/inc

mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/${configFile} ${install_dir}/cfg/${configFile}
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/*

if [ -f ${build_dir}/bin/jemalloc-config ]; then
  mkdir -p ${install_dir}/jemalloc/{bin,lib,lib/pkgconfig,include/jemalloc,share/doc/jemalloc,share/man/man3}
  cp ${build_dir}/bin/jemalloc-config ${install_dir}/jemalloc/bin
  if [ -f ${build_dir}/bin/jemalloc.sh ]; then
    cp ${build_dir}/bin/jemalloc.sh ${install_dir}/jemalloc/bin
  fi
  if [ -f ${build_dir}/bin/jeprof ]; then
    cp ${build_dir}/bin/jeprof ${install_dir}/jemalloc/bin
  fi
  if [ -f ${build_dir}/include/jemalloc/jemalloc.h ]; then
    cp ${build_dir}/include/jemalloc/jemalloc.h ${install_dir}/jemalloc/include/jemalloc
  fi
  if [ -f ${build_dir}/lib/libjemalloc.so.2 ]; then
    cp ${build_dir}/lib/libjemalloc.so.2 ${install_dir}/jemalloc/lib
    ln -sf libjemalloc.so.2 ${install_dir}/jemalloc/lib/libjemalloc.so
  fi
  # if [ -f ${build_dir}/lib/libjemalloc.a ]; then
  #   cp ${build_dir}/lib/libjemalloc.a ${install_dir}/jemalloc/lib
  # fi
  # if [ -f ${build_dir}/lib/libjemalloc_pic.a ]; then
  #   cp ${build_dir}/lib/libjemalloc_pic.a ${install_dir}/jemalloc/lib
  # fi
  if [ -f ${build_dir}/lib/pkgconfig/jemalloc.pc ]; then
    cp ${build_dir}/lib/pkgconfig/jemalloc.pc ${install_dir}/jemalloc/lib/pkgconfig
  fi
  if [ -f ${build_dir}/share/doc/jemalloc/jemalloc.html ]; then
    cp ${build_dir}/share/doc/jemalloc/jemalloc.html ${install_dir}/jemalloc/share/doc/jemalloc
  fi
  if [ -f ${build_dir}/share/man/man3/jemalloc.3 ]; then
    cp ${build_dir}/share/man/man3/jemalloc.3 ${install_dir}/jemalloc/share/man/man3
  fi
fi

cd ${install_dir}

if [ "$osType" != "Darwin" ]; then
  tar -zcv -f ${tarName} * --remove-files || :
else
  tar -zcv -f ${tarName} * || :
  mv ${tarName} ..
  rm -rf ./*
  mv ../${tarName} .
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
cp ${install_dir}/install_client.sh install_client_temp.sh
if [ "$osType" == "Darwin" ]; then
  sed -i 's/osType=Linux/osType=Darwin/g' install_client_temp.sh
  mv install_client_temp.sh ${install_dir}/install_client.sh
fi

if [ "$verMode" == "cluster" ]; then
  sed -i 's/verMode=edge/verMode=cluster/g' install_client_temp.sh
  sed -i "s/serverName2=\"taosd\"/serverName2=\"${serverName2}\"/g" install_client_temp.sh
  sed -i "s/clientName2=\"taos\"/clientName2=\"${clientName2}\"/g" install_client_temp.sh
  sed -i "s/configFile2=\"taos.cfg\"/configFile2=\"${clientName2}.cfg\"/g" install_client_temp.sh
  sed -i "s/productName2=\"TDengine\"/productName2=\"${productName2}\"/g" install_client_temp.sh
  sed -i "s/emailName2=\"taosdata.com\"/emailName2=\"${cusEmail2}\"/g" install_client_temp.sh

  mv install_client_temp.sh ${install_dir}/install_client.sh
fi
if [ "$verMode" == "cloud" ]; then
  sed -i 's/verMode=edge/verMode=cloud/g' install_client_temp.sh
  mv install_client_temp.sh ${install_dir}/install_client.sh
fi

if [ "$pagMode" == "lite" ]; then
  sed -i 's/pagMode=full/pagMode=lite/g' install_client_temp.sh
  mv install_client_temp.sh ${install_dir}/install_client.sh
fi
chmod a+x ${install_dir}/install_client.sh

if [[ $productName == "TDengine" ]] && [ "$verMode" != "cloud" ]; then
  # Copy example code
  mkdir -p ${install_dir}/examples
  examples_dir="${top_dir}/examples"
  cp -r ${examples_dir}/c ${install_dir}/examples
  if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
    cp -r ${examples_dir}/JDBC ${install_dir}/examples
    cp -r ${examples_dir}/matlab ${install_dir}/examples
    cp -r ${examples_dir}/python ${install_dir}/examples
    cp -r ${examples_dir}/R ${install_dir}/examples
    cp -r ${examples_dir}/go ${install_dir}/examples
    cp -r ${examples_dir}/nodejs ${install_dir}/examples
    cp -r ${examples_dir}/C# ${install_dir}/examples
    mkdir -p ${install_dir}/examples/taosbenchmark-json && cp ${examples_dir}/../tools/taos-tools/example/* ${install_dir}/examples/taosbenchmark-json
  fi

  if [ "$verMode" == "cluster" ]; then
      # Copy connector
      connector_dir="${code_dir}/connector"
      mkdir -p ${install_dir}/connector
      if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
          if [ "$osType" != "Darwin" ]; then
              jars=$(ls ${build_dir}/lib/*.jar 2>/dev/null|wc -l)
              [ "${jars}" != "0" ] && cp ${build_dir}/lib/*.jar ${install_dir}/connector || :
          fi
          git clone --depth 1 https://github.com/taosdata/driver-go ${install_dir}/connector/go
          rm -rf ${install_dir}/connector/go/.git ||:

          git clone --depth 1 https://github.com/taosdata/taos-connector-python ${install_dir}/connector/python
          rm -rf ${install_dir}/connector/python/.git ||:
#          cp -r ${connector_dir}/python ${install_dir}/connector
          git clone --depth 1 https://github.com/taosdata/taos-connector-node ${install_dir}/connector/nodejs
          rm -rf ${install_dir}/connector/nodejs/.git ||:

          git clone --depth 1 https://github.com/taosdata/taos-connector-dotnet ${install_dir}/connector/dotnet
          rm -rf ${install_dir}/connector/dotnet/.git ||:
#          cp -r ${connector_dir}/nodejs ${install_dir}/connector
          git clone --depth 1 https://github.com/taosdata/taos-connector-rust ${install_dir}/connector/rust
          rm -rf ${install_dir}/connector/rust/.git ||:
      fi
  fi
fi
# Copy driver
mkdir -p ${install_dir}/driver
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector
[ -f ${wslib_files} ] && cp ${wslib_files} ${install_dir}/driver

if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  if [ "$osType" != "Darwin" ]; then
    cp ${build_dir}/lib/*.jar ${install_dir}/connector || :
  fi
  if find ${connector_dir}/go -mindepth 1 -maxdepth 1 | read; then
    cp -r ${connector_dir}/go ${install_dir}/connector
  else
    echo "WARNING: go connector not found, please check if want to use it!"
  fi
  cp -r ${connector_dir}/python ${install_dir}/connector || :
  cp -r ${connector_dir}/nodejs ${install_dir}/connector || :
fi
# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir}

#  install_dir has been distinguishes  cluster from  edege, so comments this code
pkg_name=${install_dir}-${osType}-${cpuType}

# if [ "$verMode" == "cluster" ]; then
#   pkg_name=${install_dir}-${osType}-${cpuType}
# elif [ "$verMode" == "edge" ]; then
#   pkg_name=${install_dir}-${osType}-${cpuType}
# else
#   echo "unknow verMode, nor cluster or edge"
#   exit 1
# fi

if [[ "$verType" == "beta" ]] || [[ "$verType" == "preRelease" ]]; then
  pkg_name=${install_dir}-${verType}-${osType}-${cpuType}
elif [ "$verType" == "stable" ]; then
  pkg_name=${pkg_name}
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

if [ "$pagMode" == "lite" ]; then
  pkg_name=${pkg_name}-Lite
fi

if [ "$osType" != "Darwin" ]; then
  tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) --remove-files || :
else
  tar -zcv -f "$(basename ${pkg_name}).tar.gz" $(basename ${install_dir}) || :
#  mv "$(basename ${pkg_name}).tar.gz" ..
  rm -rf ${install_dir} ||:
#  mv ../"$(basename ${pkg_name}).tar.gz" .
fi

cd ${curr_dir}
