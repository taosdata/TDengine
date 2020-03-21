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
  echo "Not support dbname: ${dbName}, exit"
  exit 1
fi

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"

# create compressed install file.
build_dir="${compile_dir}/build"
code_dir="${top_dir}/src"
release_dir="${top_dir}/release"

#package_name='linux'
if [ "$verMode" == "cluster" ]; then
    install_dir="${release_dir}/${DB_FULL_NAME}-enterprise-server"
else
    install_dir="${release_dir}/${DB_FULL_NAME}-server"
fi

# Directories and files.
if [ "$pagMode" == "lite" ]; then
  strip ${build_dir}/bin/taosd 
  strip ${build_dir}/bin/taos
  bin_files="${build_dir}/bin/${DB_SERVICE_NAME} ${build_dir}/bin/${DB_CLIENT_NAME} ${script_dir}/remove.sh"
else 
  bin_files="${build_dir}/bin/${DB_SERVICE_NAME} ${build_dir}/bin/${DB_CLIENT_NAME} ${build_dir}/bin/${DB_CLIENT_NAME}demo ${build_dir}/bin/${DB_CLIENT_NAME}dump ${script_dir}/remove.sh"
fi

if [ "$verMode" == "cluster" ]; then
  lib_files="${build_dir}/lib/libtaos.so.${version} ${build_dir}/lib/libtaosodbc.so"
else 
  lib_files="${build_dir}/lib/libtaos.so.${version}"
fi

header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
cfg_dir="${top_dir}/packaging/cfg"
install_files="${script_dir}/install.sh"
nginx_dir="${code_dir}/../../enterprise/src/modules/web"

# Init file
#init_dir=${script_dir}/deb
#if [ $package_type = "centos" ]; then
#    init_dir=${script_dir}/rpm
#fi
#init_files=${init_dir}/taosd
# temp use rpm's taosd. TODO: later modify according to os type
init_file_deb=${script_dir}/../deb/taosd
init_file_rpm=${script_dir}/../rpm/taosd

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/taos.cfg ${install_dir}/cfg/${DB_CLIENT_NAME}.cfg
mkdir -p ${install_dir}/bin && cp ${bin_files} ${install_dir}/bin && chmod a+x ${install_dir}/bin/*
mkdir -p ${install_dir}/init.d && cp ${init_file_deb} ${install_dir}/init.d/${DB_SERVICE_NAME}.deb
mkdir -p ${install_dir}/init.d && cp ${init_file_rpm} ${install_dir}/init.d/${DB_SERVICE_NAME}.rpm

if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove.sh >> remove_temp.sh
    mv remove_temp.sh ${install_dir}/bin/remove.sh
    
    mkdir -p ${install_dir}/nginxd && cp -r ${nginx_dir}/* ${install_dir}/nginxd
    cp ${nginx_dir}/png/${DB_CLIENT_NAME}.png ${install_dir}/nginxd/admin/images/${DB_CLIENT_NAME}.png
    rm -rf ${install_dir}/nginxd/png

    sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/nginxd/admin/*.html
    sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/nginxd/admin/*.html
    sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/nginxd/admin/*.html
    sed -i "s/DB_COPYRIGHT/${DB_COPYRIGHT}/g"       ${install_dir}/nginxd/admin/*.html
    sed -i "s/DB_COMPANY/${DB_COMPANY}/g"           ${install_dir}/nginxd/admin/*.html
    
    sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/nginxd/admin/js/*.js
    sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/nginxd/admin/js/*.js
    sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/nginxd/admin/js/*.js
    sed -i "s/DB_COPYRIGHT/${DB_COPYRIGHT}/g"       ${install_dir}/nginxd/admin/js/*.js
    sed -i "s/DB_COMPANY/${DB_COMPANY}/g"           ${install_dir}/nginxd/admin/js/*.js
    
    sed -i "s/taosdata/${DB_COMPANY}/g"              ${install_dir}/cfg/${DB_CLIENT_NAME}.cfg
    sed -i "s/taos/${DB_CLIENT_NAME}/g"              ${install_dir}/cfg/${DB_CLIENT_NAME}.cfg
    sed -i "s/TDengine/${DB_FULL_NAME}/g"            ${install_dir}/cfg/${DB_CLIENT_NAME}.cfg
    
    sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/bin/remove.sh
    sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/bin/remove.sh
    sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/bin/remove.sh
    
    sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/init.d/${DB_SERVICE_NAME}.deb
    sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/init.d/${DB_SERVICE_NAME}.deb
    sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/init.d/${DB_SERVICE_NAME}.deb
    
    sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/init.d/${DB_SERVICE_NAME}.rpm
    sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/init.d/${DB_SERVICE_NAME}.rpm
    sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/init.d/${DB_SERVICE_NAME}.rpm
    
    #sed -i "s/TAOS Data/${DB_COPYRIGHT}/g"           ${install_dir}/inc/${DB_CLIENT_NAME}.h
    #sed -i "s/taosdata/${DB_COMPANY}/g"              ${install_dir}/inc/${DB_CLIENT_NAME}.h


    if [ "$cpuType" == "aarch64" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/64bit/nginx ${install_dir}/nginxd/sbin/
    elif [ "$cpuType" == "aarch32" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/32bit/nginx ${install_dir}/nginxd/sbin/
    fi
    rm -rf ${install_dir}/nginxd/sbin/arm
fi

cd ${install_dir}
tar -zcv -f ${DB_CLIENT_NAME}.tar.gz * --remove-files  || :
exitcode=$?
echo "tar return code: ${exitcode}"
if [ "$exitcode" != "0" ]; then
    echo "tar ${DB_CLIENT_NAME}.tar.gz error !!!"
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

sed -i "s/DB_CLIENT_NAME/${DB_CLIENT_NAME}/g"   ${install_dir}/install.sh
sed -i "s/DB_SERVICE_NAME/${DB_SERVICE_NAME}/g" ${install_dir}/install.sh
sed -i "s/DB_FULL_NAME/${DB_FULL_NAME}/g"       ${install_dir}/install.sh

chmod a+x ${install_dir}/install.sh

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
if [ "$verMode" == "cluster" ]; then
  mkdir -p ${install_dir}/examples/ODBC
  odbc_dir="${top_dir}/../enterprise/tests/examples/ODBC"
  cp -r ${odbc_dir}/testodbc.c   ${install_dir}/examples/ODBC 
  cp -r ${odbc_dir}/makefile     ${install_dir}/examples/ODBC 
fi

# Copy driver
mkdir -p ${install_dir}/driver 
cp ${lib_files} ${install_dir}/driver

# Copy connector
connector_dir="${code_dir}/connector"
mkdir -p ${install_dir}/connector
if [[ "$pagMode" != "lite" ]] && [[ "$cpuType" != "aarch32" ]]; then
  cp ${build_dir}/lib/*jdbc*.jar      ${install_dir}/connector
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
