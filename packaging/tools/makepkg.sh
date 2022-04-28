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
dbName=${10}

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"

productName="TDengine"
serverName="taosd"
clientName="taos"
configFile="taos.cfg"
tarName="taos.tar.gz"
dumpName="taosdump"
benchmarkName="taosBenchmark"
toolsName="taostools"
adapterName="taosadapter"
defaultPasswd="taosdata"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'
if [ "$verMode" == "cluster" ]; then
  install_dir="${release_dir}/${productName}-enterprise-server-${version}"
else
  install_dir="${release_dir}/${productName}-server-${version}"
fi

if [ -d ${top_dir}/src/kit/taos-tools/packaging/deb ]; then
  cd ${top_dir}/src/kit/taos-tools/packaging/deb
  [ -z "$taos_tools_ver" ] && taos_tools_ver="0.1.0"

  taostools_ver=$(git describe --tags | sed -e 's/ver-//g' | awk -F '-' '{print $1}')
  taostools_install_dir="${release_dir}/${clientName}Tools-${taostools_ver}"

  cd ${curr_dir}
else
  taostools_install_dir="${release_dir}/${clientName}Tools-${version}"
fi

# Directories and files
if [ "$pagMode" == "lite" ]; then
  strip ${build_dir}/bin/${serverName}
  strip ${build_dir}/bin/${clientName}
  # lite version doesn't include taosadapter,  which will lead to no restful interface
  bin_files="${build_dir}/bin/${serverName} ${build_dir}/bin/${clientName} ${script_dir}/remove.sh ${script_dir}/startPre.sh ${build_dir}/bin/${benchmarkName}"
  taostools_bin_files=""
else


  wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh -O ${build_dir}/bin/TDinsight.sh &&
    echo "TDinsight.sh downloaded!" ||
    echo "failed to download TDinsight.sh"

  # download TDinsight caches
  orig_pwd=$(pwd)
  tdinsight_caches=""
  cd ${build_dir}/bin/ && \
    chmod +x TDinsight.sh
  tdinsight_caches=$(./TDinsight.sh --download-only | xargs -i printf "${build_dir}/bin/{} ")
  cd $orig_pwd
  echo "TDinsight caches: $tdinsight_caches"

  taostools_bin_files=" ${build_dir}/bin/${dumpName} \
      ${build_dir}/bin/${benchmarkName} \
      ${build_dir}/bin/TDinsight.sh \
      $tdinsight_caches"

  #!!! do not change taosadapter here
  bin_files="${build_dir}/bin/${serverName} \
      ${build_dir}/bin/${clientName} \
      ${taostools_bin_files} \
      ${build_dir}/bin/taosadapter \
      ${build_dir}/bin/tarbitrator\
      ${script_dir}/remove.sh \
      ${script_dir}/set_core.sh \
      ${script_dir}/run_taosd_and_taosadapter.sh \
      ${script_dir}/startPre.sh \
      ${script_dir}/taosd-dump-cfg.gdb"
fi

lib_files="${build_dir}/lib/libtaos.so.${version}"
header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taosdef.h ${code_dir}/inc/taoserror.h"

if [ "$dbName" != "taos" ]; then
  cfg_dir="${top_dir}/../enterprise/packaging/cfg"
else
  cfg_dir="${top_dir}/packaging/cfg"
fi

install_files="${script_dir}/install.sh"
nginx_dir="${code_dir}/../../enterprise/src/plugins/web"

init_file_deb=${script_dir}/../deb/taosd
init_file_rpm=${script_dir}/../rpm/taosd
init_file_tarbitrator_deb=${script_dir}/../deb/tarbitratord
init_file_tarbitrator_rpm=${script_dir}/../rpm/tarbitratord

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/${configFile} ${install_dir}/cfg/${configFile}

# !!! do not change the taosadapter here!!!
if [ -f "${compile_dir}/test/cfg/taosadapter.toml" ]; then
  cp ${compile_dir}/test/cfg/taosadapter.toml ${install_dir}/cfg || :
fi
# !!! do not change the taosadapter here!!!
if [ -f "${compile_dir}/test/cfg/taosadapter.service" ]; then
  cp ${compile_dir}/test/cfg/taosadapter.service ${install_dir}/cfg || :
fi

if [ -f "${cfg_dir}/${serverName}.service" ]; then
  cp ${cfg_dir}/${serverName}.service ${install_dir}/cfg || :
fi

if [ -f "${top_dir}/packaging/cfg/tarbitratord.service" ]; then
  cp ${top_dir}/packaging/cfg/tarbitratord.service ${install_dir}/cfg || :
fi

if [ -f "${top_dir}/packaging/cfg/nginxd.service" ]; then
  cp ${top_dir}/packaging/cfg/nginxd.service ${install_dir}/cfg || :
fi

mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/* || :
mkdir -p ${install_dir}/init.d && cp ${init_file_deb} ${install_dir}/init.d/${serverName}.deb
mkdir -p ${install_dir}/init.d && cp ${init_file_rpm} ${install_dir}/init.d/${serverName}.rpm
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_deb} ${install_dir}/init.d/tarbitratord.deb || :
mkdir -p ${install_dir}/init.d && cp ${init_file_tarbitrator_rpm} ${install_dir}/init.d/tarbitratord.rpm || :

# !!! do not change taosadaptor here
if [ $adapterName != "taosadapter" ]; then
  # !!! do not change taosadaptor here
  mv ${install_dir}/cfg/taosadapter.toml ${install_dir}/cfg/$adapterName.toml
  sed -i "s/path = \"\/var\/log\/taos\"/path = \"\/var\/log\/${productName}\"/g" ${install_dir}/cfg/$adapterName.toml
  sed -i "s/password = \"taosdata\"/password = \"${defaultPasswd}\"/g" ${install_dir}/cfg/$adapterName.toml
  # !!! do not change taosadaptor here
  mv ${install_dir}/cfg/taosadapter.service ${install_dir}/cfg/$adapterName.service
  sed -i "s/TDengine/${productName}/g" ${install_dir}/cfg/$adapterName.service
  sed -i "s/taosAdapter/${adapterName}/g" ${install_dir}/cfg/$adapterName.service
  sed -i "s/taosadapter/${adapterName}/g" ${install_dir}/cfg/$adapterName.service
  # !!! do not change taosadaptor here
  mv ${install_dir}/bin/taosadapter ${install_dir}/bin/${adapterName}
  mv ${install_dir}/bin/run_taosd_and_taosadapter.sh ${install_dir}/bin/run_${serverName}_and_${adapterName}.sh
  mv ${install_dir}/bin/taosd-dump-cfg.gdb ${install_dir}/bin/${serverName}-dump-cfg.gdb
fi

if [ -n "${taostools_bin_files}" ]; then
  mkdir -p ${taostools_install_dir} || echo -e "failed to create ${taostools_install_dir}"
  mkdir -p ${taostools_install_dir}/bin &&
    cp ${taostools_bin_files} ${taostools_install_dir}/bin &&
    chmod a+x ${taostools_install_dir}/bin/* || :

  if [ -f ${top_dir}/src/kit/taos-tools/packaging/tools/install-${toolsName}.sh ]; then
    cp ${top_dir}/src/kit/taos-tools/packaging/tools/install-${toolsName}.sh \
      ${taostools_install_dir}/ >/dev/null &&
      chmod a+x ${taostools_install_dir}/install-${toolsName}.sh ||
      echo -e "failed to copy install-${toolsName}.sh"
  else
    echo -e "install-${toolsName}.sh not found"
  fi

  if [ -f ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-${toolsName}.sh ]; then
    cp ${top_dir}/src/kit/taos-tools/packaging/tools/uninstall-${toolsName}.sh \
      ${taostools_install_dir}/ >/dev/null &&
      chmod a+x ${taostools_install_dir}/uninstall-${toolsName}.sh ||
      echo -e "failed to copy uninstall-${toolsName}.sh"
  else
    echo -e "uninstall-${toolsName}.sh not found"
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
  sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove.sh >>remove_temp.sh
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
tar -zcv -f ${tarName} * --remove-files || :
exitcode=$?
if [ "$exitcode" != "0" ]; then
  echo "tar ${tarName} error !!!"
  exit $exitcode
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$verMode" == "cluster" ]; then
  sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/install.sh >>install_temp.sh
  mv install_temp.sh ${install_dir}/install.sh
fi
if [ "$pagMode" == "lite" ]; then
  sed 's/pagMode=full/pagMode=lite/g' ${install_dir}/install.sh >>install_temp.sh
  mv install_temp.sh ${install_dir}/install.sh
fi
chmod a+x ${install_dir}/install.sh

if [[ $dbName == "taos" ]]; then
  # Copy example code
  mkdir -p ${install_dir}/examples
  examples_dir="${top_dir}/examples"
  cp -r ${examples_dir}/c ${install_dir}/examples
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

    cp -r ${examples_dir}/JDBC ${install_dir}/examples
    cp -r ${examples_dir}/matlab ${install_dir}/examples
    cp -r ${examples_dir}/python ${install_dir}/examples
    cp -r ${examples_dir}/R ${install_dir}/examples
    cp -r ${examples_dir}/go ${install_dir}/examples
    cp -r ${examples_dir}/nodejs ${install_dir}/examples
    cp -r ${examples_dir}/C# ${install_dir}/examples
    mkdir -p ${install_dir}/examples/taosbenchmark-json && cp ${examples_dir}/../src/kit/taos-tools/example/* ${install_dir}/examples/taosbenchmark-json
  fi
fi

# Copy driver
mkdir -p ${install_dir}/driver && cp ${lib_files} ${install_dir}/driver && echo "${versionComp}" >${install_dir}/driver/vercomp.txt

# Copy connector
if [ "$verMode" == "cluster" ]; then
    connector_dir="${code_dir}/connector"
    mkdir -p ${install_dir}/connector
    if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
        cp ${build_dir}/lib/*.jar ${install_dir}/connector || :
        if find ${connector_dir}/go -mindepth 1 -maxdepth 1 | read; then
            cp -r ${connector_dir}/go ${install_dir}/connector
        else
            echo "WARNING: go connector not found, please check if want to use it!"
        fi
        git clone --depth 1 https://github.com/taosdata/taos-connector-python ${install_dir}/connector/python
        rm -rf ${install_dir}/connector/python/.git ||:

        git clone --depth 1 https://github.com/taosdata/taos-connector-node ${install_dir}/connector/nodejs
        rm -rf ${install_dir}/connector/nodejs/.git ||:

        git clone --depth 1 https://github.com/taosdata/taos-connector-dotnet ${install_dir}/connector/dotnet
        rm -rf ${install_dir}/connector/dotnet/.git ||:

        git clone --depth 1 https://github.com/taosdata/libtaos-rs ${install_dir}/connector/rust
        rm -rf ${install_dir}/connector/rust/.git ||:
        # cp -r ${connector_dir}/python ${install_dir}/connector
        # cp -r ${connector_dir}/nodejs ${install_dir}/connector
    fi
fi

# Copy release note
cp ${script_dir}/release_note ${install_dir}

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
  wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh -O ${taostools_install_dir}/bin/TDinsight.sh && echo "TDinsight.sh downloaded!" || echo "failed to download TDinsight.sh"
  tar -zcv -f "$(basename ${taostools_pkg_name}).tar.gz" "$(basename ${taostools_install_dir})" --remove-files || :
  exitcode=$?
  if [ "$exitcode" != "0" ]; then
    echo "tar ${taostools_pkg_name}.tar.gz error !!!"
    exit $exitcode
  fi
fi

cd ${curr_dir}
