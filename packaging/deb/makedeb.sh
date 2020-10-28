#!/bin/bash
#
# Generate deb package for ubuntu
set -e
# set -x

#curr_dir=$(pwd)
compile_dir=$1
output_dir=$2
tdengine_ver=$3
cpuType=$4
osType=$5
verMode=$6
verType=$7

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"
pkg_dir="${top_dir}/debworkroom"

#echo "curr_dir: ${curr_dir}"
#echo "top_dir: ${top_dir}"
#echo "script_dir: ${script_dir}"
echo "compile_dir: ${compile_dir}"
echo "pkg_dir: ${pkg_dir}"

if [ -d ${pkg_dir} ]; then
	 rm -rf ${pkg_dir}
fi
mkdir -p ${pkg_dir}
cd ${pkg_dir}

libfile="libtaos.so.${tdengine_ver}"

# create install dir 
install_home_path="/usr/local/taos"
mkdir -p ${pkg_dir}${install_home_path}
mkdir -p ${pkg_dir}${install_home_path}/bin
mkdir -p ${pkg_dir}${install_home_path}/cfg
mkdir -p ${pkg_dir}${install_home_path}/connector
mkdir -p ${pkg_dir}${install_home_path}/driver
mkdir -p ${pkg_dir}${install_home_path}/examples
mkdir -p ${pkg_dir}${install_home_path}/include
mkdir -p ${pkg_dir}${install_home_path}/init.d
mkdir -p ${pkg_dir}${install_home_path}/script
 
cp ${compile_dir}/../packaging/cfg/taos.cfg         ${pkg_dir}${install_home_path}/cfg
cp ${compile_dir}/../packaging/deb/taosd            ${pkg_dir}${install_home_path}/init.d
cp ${compile_dir}/../packaging/tools/post.sh        ${pkg_dir}${install_home_path}/script
cp ${compile_dir}/../packaging/tools/preun.sh       ${pkg_dir}${install_home_path}/script
cp ${compile_dir}/build/bin/taosdemo                ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/bin/taosd                   ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/bin/taos                    ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/lib/${libfile}              ${pkg_dir}${install_home_path}/driver 
cp ${compile_dir}/../src/inc/taos.h                 ${pkg_dir}${install_home_path}/include
cp ${compile_dir}/../src/inc/taoserror.h            ${pkg_dir}${install_home_path}/include
cp -r ${top_dir}/tests/examples/*                   ${pkg_dir}${install_home_path}/examples
cp -r ${top_dir}/src/connector/grafanaplugin        ${pkg_dir}${install_home_path}/connector
cp -r ${top_dir}/src/connector/python               ${pkg_dir}${install_home_path}/connector
cp -r ${top_dir}/src/connector/go                   ${pkg_dir}${install_home_path}/connector
cp -r ${top_dir}/src/connector/nodejs               ${pkg_dir}${install_home_path}/connector
cp ${compile_dir}/build/lib/taos-jdbcdriver*dist.*  ${pkg_dir}${install_home_path}/connector ||:

cp -r ${compile_dir}/../packaging/deb/DEBIAN        ${pkg_dir}/
chmod 755 ${pkg_dir}/DEBIAN/*

# modify version of control
debver="Version: "$tdengine_ver
sed -i "2c$debver" ${pkg_dir}/DEBIAN/control
 
#get taos version, then set deb name


if [ "$verMode" == "cluster" ]; then
  debname="TDengine-server-"${tdengine_ver}-${osType}-${cpuType}
elif [ "$verMode" == "edge" ]; then
  debname="TDengine-server"-${tdengine_ver}-${osType}-${cpuType}
else
  echo "unknow verMode, nor cluster or edge"
  exit 1
fi

if [ "$verType" == "beta" ]; then
  debname=${debname}-${verType}".deb"
elif [ "$verType" == "stable" ]; then 
  debname=${debname}".deb"
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

# make deb package
dpkg -b ${pkg_dir} $debname
echo "make deb package success!"

cp ${pkg_dir}/*.deb ${output_dir} 

# clean tmep dir
rm -rf ${pkg_dir}

