#!/bin/bash
#
# Generate tar.gz package for all os system

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
versionComp=$9

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'
if [ "$verMode" == "cluster" ]; then
    install_dir="${release_dir}/TDengine-enterprise-server-${version}"
else
    install_dir="${release_dir}/TDengine-server-${version}"
fi

if [ -d ${top_dir}/src/kit/taos-tools/packaging/deb ]; then
    cd ${top_dir}/src/kit/taos-tools/packaging/deb
    [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

    taostools_ver=$(git describe --tags|sed -e 's/ver-//g'|awk -F '-' '{print $1}')
    taostools_install_dir="${release_dir}/taos-tools-${taostools_ver}"

    cd ${curr_dir}
else
    taostools_install_dir="${release_dir}/taos-tools-${version}"
fi

# Directories and files
if [ "$pagMode" == "lite" ]; then
  strip ${build_dir}/bin/taosd
  strip ${build_dir}/bin/taos
  # lite version doesn't include taosadapter,  which will lead to no restful interface
  bin_files="${build_dir}/bin/taosd ${build_dir}/bin/taos ${script_dir}/remove.sh ${script_dir}/startPre.sh"
  taostools_bin_files=""
else
  bin_files="${build_dir}/bin/taosd \
      ${build_dir}/bin/taos \
      ${build_dir}/bin/taosadapter \
      ${build_dir}/bin/tarbitrator\
      ${script_dir}/remove.sh \
      ${script_dir}/set_core.sh \
      ${script_dir}/run_taosd.sh \
      ${script_dir}/startPre.sh \
      ${script_dir}/taosd-dump-cfg.gdb"

  taostools_bin_files=" ${build_dir}/bin/taosdump \
      ${build_dir}/bin/taosBenchmark"
fi

lib_files="${build_dir}/lib/libtaos.so.${version}"
header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taosdef.h ${code_dir}/inc/taoserror.h"
if [ "$verMode" == "cluster" ]; then
  cfg_dir="${top_dir}/../enterprise/packaging/cfg"
else
  cfg_dir="${top_dir}/packaging/cfg"
fi

install_files="${script_dir}/install.sh"
nginx_dir="${code_dir}/../../enterprise/src/plugins/web"

# Init file
#init_dir=${script_dir}/deb
#if [ $package_type = "centos" ]; then
#    init_dir=${script_dir}/rpm
#fi
#init_files=${init_dir}/taosd
# temp use rpm's taosd. TODO: later modify according to os type
init_file_deb=${script_dir}/../deb/taosd
init_file_rpm=${script_dir}/../rpm/taosd
init_file_tarbitrator_deb=${script_dir}/../deb/tarbitratord
init_file_tarbitrator_rpm=${script_dir}/../rpm/tarbitratord

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/taos.cfg ${install_dir}/cfg/taos.cfg


if [ -f "${compile_dir}/test/cfg/taosadapter.toml" ]; then
    cp ${compile_dir}/test/cfg/taosadapter.toml                 ${install_dir}/cfg || :
fi

if [ -f "${compile_dir}/test/cfg/taosadapter.service" ]; then
    cp ${compile_dir}/test/cfg/taosadapter.service          ${install_dir}/cfg || :
fi

if [ -f "${cfg_dir}/taosd.service" ]; then
    cp ${cfg_dir}/taosd.service          ${install_dir}/cfg || :
fi
if [ -f "${cfg_dir}/tarbitratord.service" ]; then
    cp ${cfg_dir}/tarbitratord.service          ${install_dir}/cfg || :
fi
if [ -f "${cfg_dir}/nginxd.service" ]; then
    cp ${cfg_dir}/nginxd.service          ${install_dir}/cfg || :
fi

mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/* || :
mkdir -p ${install_dir}/init.d && cp ${init_file_deb} ${install_dir}/init.d/taosd.deb
mkdir -p ${install_dir}/init.d && cp ${init_file_rpm} ${install_dir}/init.d/taosd.rpm
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_deb} ${install_dir}/init.d/tarbitratord.deb || :
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_rpm} ${install_dir}/init.d/tarbitratord.rpm || :

if [ -n "${taostools_bin_files}" ]; then
    mkdir -p ${taostools_install_dir} || echo -e "failed to create ${taostools_install_dir}"
    mkdir -p ${taostools_install_dir}/bin \
        && cp ${taostools_bin_files} ${taostools_install_dir}/bin \
        && chmod a+x ${taostools_install_dir}/bin/* || :
    [ -f ${taostools_install_dir}/bin/taosBenchmark ] && \
        ln -sf ${taostools_install_dir}/bin/taosBenchmark \
        ${taostools_install_dir}/bin/taosdemo

    if [ -f ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh ]; then
        cp ${top_dir}/src/kit/taos-tools/packaging/tools/install-taostools.sh \
            ${taostools_install_dir}/ > /dev/null \
            && chmod a+x ${taostools_install_dir}/install-taostools.sh \
            || echo -e "failed to copy install-taostools.sh"
    else
        echo -e "install-taostools.sh not found"
    fi

    if [ -f ${build_dir}/lib/libavro.so.23.0.0 ]; then
        mkdir -p ${taostools_install_dir}/avro/{lib,lib/pkgconfig} || echo -e "failed to create ${taostools_install_dir}/avro"
        cp ${build_dir}/lib/libavro.* ${taostools_install_dir}/avro/lib
        cp ${build_dir}/lib/pkgconfig/avro-c.pc ${taostools_install_dir}/avro/lib/pkgconfig
    fi
fi

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

if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove.sh >> remove_temp.sh
    mv remove_temp.sh ${install_dir}/bin/remove.sh

    mkdir -p ${install_dir}/nginxd && cp -r ${nginx_dir}/* ${install_dir}/nginxd
    cp ${nginx_dir}/png/taos.png ${install_dir}/nginxd/admin/images/taos.png
    rm -rf ${install_dir}/nginxd/png

    if [ "$cpuType" == "aarch64" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/64bit/nginx ${install_dir}/nginxd/sbin/
    elif [ "$cpuType" == "aarch32" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/32bit/nginx ${install_dir}/nginxd/sbin/
    fi
    rm -rf ${install_dir}/nginxd/sbin/arm
fi

cd ${install_dir}
tar -zcv -f taos.tar.gz * --remove-files  || :
exitcode=$?
if [ "$exitcode" != "0" ]; then
    echo "tar taos.tar.gz error !!!"
    exit $exitcode
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/install.sh >> install_temp.sh
    mv install_temp.sh ${install_dir}/install.sh
fi
if [ "$pagMode" == "lite" ]; then
    sed 's/pagMode=full/pagMode=lite/g' ${install_dir}/install.sh >> install_temp.sh
    mv install_temp.sh ${install_dir}/install.sh
fi
chmod a+x ${install_dir}/install.sh

# Copy example code
mkdir -p ${install_dir}/examples
examples_dir="${top_dir}/tests/examples"
  cp -r ${examples_dir}/c      ${install_dir}/examples
if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  if [ -d ${examples_dir}/JDBC/connectionPools/target ]; then
    rm -rf ${examples_dir}/JDBC/connectionPools/target
  fi
  if [ -d ${examples_dir}/JDBC/JDBCDemo/target ]; then
    rm -rf ${examples_dir}/JDBC/JDBCDemo/target
  fi
  if [ -d ${examples_dir}/JDBC/mybatisplus-demo/target ]; then
    rm -rf ${examples_dir}/JDBC/mybatisplus-demo/target
  fi
  if [ -d ${examples_dir}/JDBC/springbootdemo/target ]; then
    rm -rf ${examples_dir}/JDBC/springbootdemo/target
  fi
  if [ -d ${examples_dir}/JDBC/SpringJdbcTemplate/target ]; then
    rm -rf ${examples_dir}/JDBC/SpringJdbcTemplate/target
  fi
  if [ -d ${examples_dir}/JDBC/taosdemo/target ]; then
    rm -rf ${examples_dir}/JDBC/taosdemo/target
  fi

  cp -r ${examples_dir}/JDBC   ${install_dir}/examples
  cp -r ${examples_dir}/matlab ${install_dir}/examples
  cp -r ${examples_dir}/python ${install_dir}/examples
  cp -r ${examples_dir}/R      ${install_dir}/examples
  cp -r ${examples_dir}/go     ${install_dir}/examples
  cp -r ${examples_dir}/nodejs ${install_dir}/examples
  cp -r ${examples_dir}/C#     ${install_dir}/examples
fi
# Copy driver
mkdir -p ${install_dir}/driver && cp ${lib_files} ${install_dir}/driver && echo "${versionComp}" > ${install_dir}/driver/vercomp.txt

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector
if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  cp ${build_dir}/lib/*.jar            ${install_dir}/connector ||:
  if find ${connector_dir}/go -mindepth 1 -maxdepth 1 | read; then
    cp -r ${connector_dir}/go ${install_dir}/connector
  else
    echo "WARNING: go connector not found, please check if want to use it!"
  fi
  cp -r ${connector_dir}/python        ${install_dir}/connector
  cp -r ${connector_dir}/nodejs        ${install_dir}/connector
fi
# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1

cd ${release_dir}

#  install_dir has been distinguishes  cluster from  edege, so comments this code
pkg_name=${install_dir}-${osType}-${cpuType}

taostools_pkg_name=${taostools_install_dir}-${osType}-${cpuType}

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
  taostools_pkg_name=${taostools_install_dir}-${verType}-${osType}-${cpuType}
elif [ "$verType" == "stable" ]; then
  pkg_name=${pkg_name}
  taostools_pkg_name=${taostools_pkg_name}
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

if [ "$pagMode" == "lite" ]; then
  pkg_name=${pkg_name}-Lite
fi

tar -zcv -f "$(basename ${pkg_name}).tar.gz" "$(basename ${install_dir})" --remove-files || :
exitcode=$?
if [ "$exitcode" != "0" ]; then
    echo "tar ${pkg_name}.tar.gz error !!!"
    exit $exitcode
fi

if [ -n "${taostools_bin_files}" ]; then
    tar -zcv -f "$(basename ${taostools_pkg_name}).tar.gz" "$(basename ${taostools_install_dir})" --remove-files || :
    exitcode=$?
    if [ "$exitcode" != "0" ]; then
        echo "tar ${taostools_pkg_name}.tar.gz error !!!"
        exit $exitcode
    fi
fi

cd ${curr_dir}
