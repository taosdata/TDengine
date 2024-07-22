#!/bin/bash
#
# Generate rpm package for centos

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
pkg_dir="${top_dir}/rpmworkroom"
spec_file="${script_dir}/tdengine.spec"

#echo "curr_dir: ${curr_dir}"
#echo "top_dir: ${top_dir}"
#echo "script_dir: ${script_dir}"
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
    echo "cp_rpm_package cd: ${cur_dir}"
    for dirlist in "$(ls ${cur_dir})"; do
        if test -d ${dirlist}; then
            cd ${dirlist}
            echo 'cp_rpm_package ${cur_dir}/${dirlist}'
            cp_rpm_package ${cur_dir}/${dirlist}
            cd ..
        fi
        if test -e ${dirlist}; then
            echo "${cur_dir}/${dirlist} ${output_dir}/TDengine-${tdengine_ver}.rpm"
            cp ${cur_dir}/${dirlist} ${output_dir}/TDengine-${tdengine_ver}.rpm
        fi
    done
}

if [ -d ${pkg_dir} ]; then
  ${csudo}rm -rf ${pkg_dir}
fi
${csudo}mkdir -p ${pkg_dir}
cd ${pkg_dir}

# download taoskeeper and build
if [ "$cpuType" = "x64" ] || [ "$cpuType" = "x86_64" ] || [ "$cpuType" = "amd64" ]; then
  arch=amd64
elif [ "$cpuType" = "x32" ] || [ "$cpuType" = "i386" ] || [ "$cpuType" = "i686" ]; then
  arch=386
elif [ "$cpuType" = "arm" ] || [ "$cpuType" = "aarch32" ]; then
  arch=arm
elif [ "$cpuType" = "arm64" ] || [ "$cpuType" = "aarch64" ]; then
  arch=arm64
else
  arch=$cpuType
fi

cd ${top_dir}
echo "${top_dir}/../enterprise/packaging/build_taoskeeper.sh -r ${arch} -e taoskeeper -t ver-${tdengine_ver}"
taoskeeper_binary=`${top_dir}/../enterprise/packaging/build_taoskeeper.sh -r $arch -e taoskeeper -t ver-${tdengine_ver}`
echo "taoskeeper_binary: ${taoskeeper_binary}"
cd ${package_dir}

${csudo}mkdir -p BUILD BUILDROOT RPMS SOURCES SPECS SRPMS

${csudo}rpmbuild --define="_version ${tdengine_ver}" --define="_topdir ${pkg_dir}" --define="_compiledir ${compile_dir}" -bb ${spec_file}

# copy rpm package to output_dir, and modify package name, then clean temp dir
#${csudo}cp -rf RPMS/* ${output_dir}
cp_rpm_package ${pkg_dir}/RPMS


if [ "$verMode" == "cluster" ]; then
  rpmname="TDengine-server-"${tdengine_ver}-${osType}-${cpuType}
elif [ "$verMode" == "edge" ]; then
  rpmname="TDengine-server"-${tdengine_ver}-${osType}-${cpuType}
else
  echo "unknow verMode, nor cluster or edge"
  exit 1
fi

if [ "$verType" == "beta" ]; then
  rpmname="TDengine-server-"${tdengine_ver}-${verType}-${osType}-${cpuType}".rpm"
elif [ "$verType" == "stable" ]; then
  rpmname=${rpmname}".rpm"
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

mv ${output_dir}/TDengine-${tdengine_ver}.rpm ${output_dir}/${rpmname}

cd ..
${csudo}rm -rf ${pkg_dir}
rm -rf ${top_dir}/build-taoskeeper