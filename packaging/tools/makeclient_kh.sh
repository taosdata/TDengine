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
    install_dir="${release_dir}/KingHistorian-enterprise-client-${version}"
else
    install_dir="${release_dir}/KingHistorian-client-${version}"
fi

# Directories and files.

if [ "$osType" != "Darwin" ]; then
  lib_files="${build_dir}/lib/libtaos.so.${version}"
else
  bin_files="${build_dir}/bin/taos ${script_dir}/remove_client_kh.sh"
  lib_files="${build_dir}/lib/libtaos.${version}.dylib"
fi

header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
if [ "$verMode" == "cluster" ]; then
  cfg_dir="${top_dir}/../enterprise/packaging/cfg"
else
  cfg_dir="${top_dir}/packaging/cfg"
fi

install_files="${script_dir}/install_client_kh.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/kinghistorian.cfg ${install_dir}/cfg/kinghistorian.cfg

sed -i '/dataDir/ {s/taos/kinghistorian/g}'  ${install_dir}/cfg/kinghistorian.cfg
sed -i '/logDir/  {s/taos/kinghistorian/g}'  ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/TDengine/KingHistorian/g"        ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/TAOS/KingHistorian/g"      ${install_dir}/cfg/kinghistorian.cfg

mkdir -p ${install_dir}/bin
if [ "$osType" != "Darwin" ]; then
  if [ "$pagMode" == "lite" ]; then
    strip ${build_dir}/bin/taos
    cp ${build_dir}/bin/taos          ${install_dir}/bin/khclient
    cp ${script_dir}/remove_client_kh.sh  ${install_dir}/bin
  else 
    cp ${build_dir}/bin/taos          ${install_dir}/bin/khclient
    cp ${script_dir}/remove_client_kh.sh  ${install_dir}/bin
    cp ${build_dir}/bin/taosdemo      ${install_dir}/bin/khdemo
    cp ${build_dir}/bin/taosdump      ${install_dir}/bin/khdump
    cp ${script_dir}/set_core.sh      ${install_dir}/bin
    cp ${script_dir}/get_client.sh    ${install_dir}/bin
    #cp ${script_dir}/taosd-dump-cfg.gdb    ${install_dir}/bin
  fi
else
  cp ${bin_files} ${install_dir}/bin
fi
chmod a+x ${install_dir}/bin/* || :

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
    if [ -f ${build_dir}/lib/libjemalloc.a ]; then
        cp ${build_dir}/lib/libjemalloc.a ${install_dir}/jemalloc/lib
    fi
    if [ -f ${build_dir}/lib/libjemalloc_pic.a ]; then
        cp ${build_dir}/lib/libjemalloc_pic.a ${install_dir}/jemalloc/lib
    fi
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
    tar -zcv -f kinghistorian.tar.gz * --remove-files || :
else
    tar -zcv -f kinghistorian.tar.gz * || :
    mv kinghistorian.tar.gz ..
    rm -rf ./*
    mv ../kinghistorian.tar.gz .
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$osType" == "Darwin" ]; then
    sed 's/osType=Linux/osType=Darwin/g' ${install_dir}/install_client_kh.sh >> install_client_kh_temp.sh
    mv install_client_kh_temp.sh ${install_dir}/install_client_kh.sh
fi
if [ "$pagMode" == "lite" ]; then
    sed 's/pagMode=full/pagMode=lite/g' ${install_dir}/install_client_kh.sh >> install_client_kh_temp.sh
    mv install_client_kh_temp.sh ${install_dir}/install_client_kh.sh
fi
chmod a+x ${install_dir}/install_client_kh.sh

# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

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
