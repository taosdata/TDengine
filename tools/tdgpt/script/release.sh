#!/bin/bash
# Generate install package for all os system

set -e
# set -x

curr_dir=$(pwd)
compile_dir=$1
version="1.0.1"
osType=
pagMode=
productName="TDengine-enterprise-anode"

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/..)"

echo -e ${top_dir}

serverName="taosanoded"
configFile="taosanode.ini"
tarName="package.tar.gz"

# create compressed install file.
build_dir="${compile_dir}/build"
release_dir="${top_dir}/release"

#package_name='linux'
install_dir="${release_dir}/${productName}-${version}"

if [ "$pagMode" == "lite" ]; then
  strip ${build_dir}/bin/${serverName}
fi

cfg_dir="${top_dir}/cfg"
install_files="${script_dir}/install.sh"

# make directories.
mkdir -p ${install_dir}
mkdir -p ${install_dir}/cfg && cp ${cfg_dir}/${configFile} ${install_dir}/cfg/${configFile}

if [ -f "${cfg_dir}/${serverName}.service" ]; then
  cp ${cfg_dir}/${serverName}.service ${install_dir}/cfg || :
fi

# python files
mkdir -p ${install_dir}/bin && mkdir -p ${install_dir}/lib

# script to control start/stop/uninstall process
rm -r ${top_dir}/taosanalytics/*.pyc || :
cp -r ${top_dir}/taosanalytics/ ${install_dir}/lib/ && chmod a+x ${install_dir}/lib/ || :
cp -r ${top_dir}/script/st*.sh ${install_dir}/bin/ && chmod a+x ${install_dir}/bin/* || :
cp -r ${top_dir}/script/uninstall.sh ${install_dir}/bin/ && chmod a+x ${install_dir}/bin/* || :

cd ${install_dir}

#if [ "$osType" != "Darwin" ]; then
#    tar -zcv -f ${tarName} ./bin/* || :
#    rm -rf ${install_dir}/bin || :
#else
tar -zcv -f ${tarName} ./lib/* || :

if [ ! -z "${install_dir}" ]; then
  # shellcheck disable=SC2115
  rm -rf "${install_dir}"/lib || :
fi

exitcode=$?
if [ "$exitcode" != "0" ]; then
  echo "tar ${tarName} error !!!"
  exit $exitcode
fi

cd ${curr_dir}
cp ${install_files} ${install_dir}

chmod a+x ${install_dir}/install.sh

# Copy release note
# cp ${script_dir}/release_note ${install_dir}

# exit 1
cd ${release_dir}

pkg_name=${install_dir}
echo -e "pkg_name is: ${pkg_name}"

if [ "$osType" != "Darwin" ]; then
    tar -zcv -f "$(basename ${pkg_name}).tar.gz" "$(basename ${install_dir})" --remove-files || :
else
    tar -zcv -f "$(basename ${pkg_name}).tar.gz" "$(basename ${install_dir})" || :
    rm -rf "${install_dir}" ||:
fi

exitcode=$?
if [ "$exitcode" != "0" ]; then
  echo "tar ${pkg_name}.tar.gz error !!!"
  exit $exitcode
fi

cd ${curr_dir}
