#!/bin/bash
#
# Generate rpm package for centos

set -e
# set -x

#curr_dir=$(pwd)
top_dir=$1
compile_dir=$2
output_dir=$3
taos_tools_ver=$4
cpuType=$5
osType=$6
verMode=$7
verType=$8
versionComp=$9

script_dir="$(dirname $(readlink -f $0))"
pkg_dir="${top_dir}/rpmworkroom"
spec_file="${script_dir}/taos-tools.spec"

#echo "curr_dir: ${curr_dir}"
#echo "top_dir: ${top_dir}"
#echo "script_dir: ${script_dir}"
echo "verMode: ${verMode}"
echo "compile_dir: ${compile_dir}"
echo "pkg_dir: ${pkg_dir}"
echo "spec_file: ${spec_file}"

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

function cp_rpm_package() {
    local cur_dir
    cd $1
    cur_dir=$(pwd)

    for dirlist in "$(ls ${cur_dir})"; do
        if test -d ${dirlist}; then
            cd ${dirlist}
            cp_rpm_package ${cur_dir}/${dirlist}
            cd ..
        fi
        if test -e ${dirlist}; then
            cp ${cur_dir}/${dirlist} ${output_dir}/taosTools-${taos_tools_ver}.rpm
        fi
    done
}

if [ -d ${pkg_dir} ]; then
  ${csudo}rm -rf ${pkg_dir}
fi
${csudo}mkdir -p ${pkg_dir}
cd ${pkg_dir}

${csudo}mkdir -p BUILD BUILDROOT RPMS SOURCES SPECS SRPMS

wget https://github.com/taosdata/grafanaplugin/releases/latest/download/TDinsight.sh -O ${compile_dir}/build/bin/TDinsight.sh && \
    echo "TDinsight.sh downloaded!" || \
    echo "failed to download TDinsight.sh"

${csudo}rpmbuild --define="_version ${taos_tools_ver}" --define="_topdir ${pkg_dir}" --define="_compiledir ${compile_dir}" -bb ${spec_file}

# copy rpm package to output_dir, and modify package name, then clean temp dir
#${csudo}cp -rf RPMS/* ${output_dir}
cp_rpm_package ${pkg_dir}/RPMS

compN=""
if [ ! -z "${versionComp}" ]; then
  versionCompFirst=$(echo "${versionComp}" | awk -F '.' '{print $1}')
  [ ! -z "${versionCompFirst}" ] && compN="-comp${versionCompFirst}"
fi

if [ "$verType" == "beta" ]; then
  rpmname="taosTools-"${taos_tools_ver}-${verType}-${osType}-${cpuType}${compN}".rpm"
elif [ "$verType" == "stable" ]; then
  rpmname="taosTools-"${taos_tools_ver}-${osType}-${cpuType}${compN}".rpm"
else
  echo "unknown verType, neither stabel nor beta"
  exit 1
fi

mv ${output_dir}/taosTools-${taos_tools_ver}.rpm ${output_dir}/${rpmname}

cd ..
${csudo}rm -rf ${pkg_dir}

