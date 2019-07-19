#!/bin/bash
#
# Generate rpm package for centos 

#curr_dir=$(pwd)
compile_dir=$1
output_dir=$2
tdengine_ver=$3

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -m ${script_dir}/../..)"
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
    csudo="sudo"
fi

if [ -d ${pkg_dir} ]; then
	 ${csudo} rm -rf ${pkg_dir}
fi
${csudo} mkdir -p ${pkg_dir}
cd ${pkg_dir}

${csudo} mkdir -p BUILD BUILDROOT RPMS SOURCES SPECS SRPMS

${csudo} rpmbuild --define="_version ${tdengine_ver}" --define="_topdir ${pkg_dir}" --define="_compiledir ${compile_dir}" -bb ${spec_file}

# copy rpm package to output_dir, then clean temp dir
#echo "rmpbuild end, cur_dir: $(pwd) "
${csudo} cp -rf RPMS/* ${output_dir} 
cd ..
${csudo} rm -rf ${pkg_dir}
