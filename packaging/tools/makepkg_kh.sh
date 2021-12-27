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

# package_name='linux'
if [ "$verMode" == "cluster" ]; then
    install_dir="${release_dir}/KingHistorian-enterprise-server-${version}"
else
    install_dir="${release_dir}/KingHistorian-server-${version}"
fi

lib_files="${build_dir}/lib/libtaos.so.${version}"
header_files="${code_dir}/inc/taos.h ${code_dir}/inc/taoserror.h"
if [ "$verMode" == "cluster" ]; then
  cfg_dir="${top_dir}/../enterprise/packaging/cfg"
else
  cfg_dir="${top_dir}/packaging/cfg"
fi
install_files="${script_dir}/install_kh.sh"
nginx_dir="${code_dir}/../../enterprise/src/plugins/web"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/inc && cp ${header_files} ${install_dir}/inc
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/kinghistorian.cfg ${install_dir}/cfg/kinghistorian.cfg
mkdir -p ${install_dir}/bin

# bin
if [ "$pagMode" == "lite" ]; then
  strip ${build_dir}/bin/taosd
  strip ${build_dir}/bin/taos
else
  cp ${build_dir}/bin/taosdemo      ${install_dir}/bin/khdemo
  cp ${build_dir}/bin/taosdump      ${install_dir}/bin/khdump
  cp ${build_dir}/bin/tarbitrator   ${install_dir}/bin
  cp ${script_dir}/set_core.sh      ${install_dir}/bin
  cp ${script_dir}/get_client.sh    ${install_dir}/bin
  cp ${script_dir}/startPre.sh      ${install_dir}/bin
  cp ${script_dir}/taosd-dump-cfg.gdb  ${install_dir}/bin
fi
cp ${build_dir}/bin/taos          ${install_dir}/bin/khclient
cp ${build_dir}/bin/taosd         ${install_dir}/bin/khserver
cp ${script_dir}/remove_kh.sh  ${install_dir}/bin
chmod a+x ${install_dir}/bin/* || :

# cluster
if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/bin/remove_kh.sh >> remove_kh_temp.sh
    mv remove_kh_temp.sh ${install_dir}/bin/remove_kh.sh

    mkdir -p ${install_dir}/nginxd && cp -r ${nginx_dir}/* ${install_dir}/nginxd
    cp ${nginx_dir}/png/taos.png ${install_dir}/nginxd/admin/images/taos.png
    rm -rf ${install_dir}/nginxd/png

    sed -i -e 's/www.taosdata.com/www.wellintech.com/g' $(grep -r 'www.taosdata.com' ${install_dir}/nginxd | sed -r "s/(.*\.html):\s*(.*)/\1/g")
    sed -i -e 's/2017/2021/g' $(grep -r '2017' ${install_dir}/nginxd | sed -r "s/(.*\.html):\s*(.*)/\1/g")
    sed -i -e 's/TAOS Data/Wellintech/g' $(grep -r 'TAOS Data' ${install_dir}/nginxd | sed -r "s/(.*\.html):\s*(.*)/\1/g")
    sed -i -e 's/taosd/khserver/g' `grep -r 'taosd' ${install_dir}/nginxd | grep -E '*\.js\s*.*' | sed -r -e 's/(.*\.js):\s*(.*)/\1/g' | sort | uniq`
    sed -i -e 's/<th style="font-weight: normal">taosd<\/th>/<th style="font-weight: normal">khserver<\/th>/g' ${install_dir}/nginxd/admin/monitor.html
    sed -i -e "s/data:\['taosd', 'system'\],/data:\['khserver', 'system'\],/g" ${install_dir}/nginxd/admin/monitor.html
    sed -i -e "s/name: 'taosd',/name: 'khserver',/g" ${install_dir}/nginxd/admin/monitor.html
    sed -i "s/TDengine/KingHistorian/g"   ${install_dir}/nginxd/admin/*.html
    sed -i "s/TDengine/KingHistorian/g"   ${install_dir}/nginxd/admin/js/*.js

    if [ "$cpuType" == "aarch64" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/64bit/nginx ${install_dir}/nginxd/sbin/
    elif [ "$cpuType" == "aarch32" ]; then
        cp -f ${install_dir}/nginxd/sbin/arm/32bit/nginx ${install_dir}/nginxd/sbin/
    fi
    rm -rf ${install_dir}/nginxd/sbin/arm
fi

sed -i '/dataDir/ {s/taos/kinghistorian/g}'  ${install_dir}/cfg/kinghistorian.cfg
sed -i '/logDir/  {s/taos/kinghistorian/g}'  ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/TDengine/KingHistorian/g"     ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/support@taosdata.com/support@wellintech.com/g"      ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/taos client/khclient/g"      ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/taosd/khserver/g"      ${install_dir}/cfg/kinghistorian.cfg
sed -i "s/TAOS/KingHistorian/g"      ${install_dir}/cfg/kinghistorian.cfg

cd ${install_dir}
tar -zcv -f kinghistorian.tar.gz * --remove-files  || :
exitcode=$?
if [ "$exitcode" != "0" ]; then
    echo "tar kinghistorian.tar.gz error !!!"
    exit $exitcode
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}
if [ "$verMode" == "cluster" ]; then
    sed 's/verMode=edge/verMode=cluster/g' ${install_dir}/install_kh.sh >> install_kh_temp.sh
    mv install_kh_temp.sh ${install_dir}/install_kh.sh
fi
if [ "$pagMode" == "lite" ]; then
    sed -e "s/pagMode=full/pagMode=lite/g" -e "s/taos_history/kh_history/g" ${install_dir}/install.sh >> install_kh_temp.sh
    mv install_kh_temp.sh ${install_dir}/install_kh.sh
fi

sed -i "/install_connector$/d" ${install_dir}/install_kh.sh
sed -i "/install_examples$/d" ${install_dir}/install_kh.sh
chmod a+x ${install_dir}/install_kh.sh

# Copy driver
mkdir -p ${install_dir}/driver && cp ${lib_files} ${install_dir}/driver && echo "${versionComp}" > ${install_dir}/driver/vercomp.txt

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
