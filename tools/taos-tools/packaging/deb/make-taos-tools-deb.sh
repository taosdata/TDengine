#!/bin/bash
#
# Generate deb package for ubuntu
set -e
# set -x

dumpName="taosdump"
#benchmarkName="taosBenchmark"
TDinsight="TDinsight.sh"

deb_dir=$(pwd)
top_dir=$1
compile_dir=$2
output_dir=$3
taos_tools_ver=$4
cpuType=$5
osType=$6
verMode=$7
verType=$8
versionComp=$9

#script_dir="$(dirname $(readlink -f $0))"
pkg_dir="${top_dir}/debworkroom"

echo "deb_dir: ${deb_dir}"
echo "verMode: ${verMode}"
#echo "top_dir: ${top_dir}"
#echo "script_dir: ${script_dir}"
echo "compile_dir: ${compile_dir}"
echo "pkg_dir: ${pkg_dir}"

if [ -d ${pkg_dir} ]; then
    rm -rf ${pkg_dir}
fi
mkdir -p ${pkg_dir}
cd ${pkg_dir}

# create install dir
install_home_path="/usr/local/taos"
mkdir -p ${pkg_dir}${install_home_path}
mkdir -p ${pkg_dir}${install_home_path}/bin || :

cp ${compile_dir}/build/bin/${dumpName}                     ${pkg_dir}${install_home_path}/bin
#cp ${compile_dir}/build/bin/${benchmarkName}                ${pkg_dir}${install_home_path}/bin

wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh -O ${compile_dir}/build/bin/${TDinsight} && \
    echo "TDinsight.sh downloaded!" || \
    echo "failed to download TDinsight.sh"

[ -f ${compile_dir}/build/bin/${TDinsight} ] && \
    chmod +x ${compile_dir}/build/bin/${TDinsight} && \
    cp ${compile_dir}/build/bin/${TDinsight}                    ${pkg_dir}${install_home_path}/bin

install_user_local_path="/usr/local"

if [ -f ${compile_dir}/build/lib/libavro.so.23.0.0 ]; then
    mkdir -p ${pkg_dir}${install_user_local_path}/lib
    cp ${compile_dir}/build/lib/libavro.so.23.0.0 ${pkg_dir}${install_user_local_path}/lib/
    ln -sf libavro.so.23.0.0 ${pkg_dir}${install_user_local_path}/lib/libavro.so.23
    ln -sf libavro.so.23 ${pkg_dir}${install_user_local_path}/lib/libavro.so
fi
if [ -f ${compile_dir}/build/lib/libavro.a ]; then
    cp ${compile_dir}/build/lib/libavro.a ${pkg_dir}${install_user_local_path}/lib/
fi

cp -r ${deb_dir}/DEBIAN ${pkg_dir}/
chmod 755 ${pkg_dir}/DEBIAN/*

compN=""
if [ ! -z "${versionComp}" ]; then
  versionCompFirst=$(echo "${versionComp}" | awk -F '.' '{print $1}')
  [ ! -z "${versionCompFirst}" ] && compN="-comp${versionCompFirst}"
fi

if [ "$verType" == "beta" ]; then
  debname="taosTools-"${taos_tools_ver}-${verType}-${osType}-${cpuType}${compN}".deb"
elif [ "$verType" == "stable" ]; then
  debname="taosTools-"${taos_tools_ver}-${osType}-${cpuType}${compN}".deb"
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

# modify version of control
debver="Version: "$taos_tools_ver
sed -i "2c$debver" ${pkg_dir}/DEBIAN/control

# make deb package
dpkg -b ${pkg_dir} $debname
echo "make taos-tools deb package success!"

cp ${pkg_dir}/*.deb ${output_dir}

# clean temp dir
rm -rf ${pkg_dir}
