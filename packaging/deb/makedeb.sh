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
product_name=$8

script_dir="$(dirname $(readlink -f $0))"
top_dir="$(readlink -f ${script_dir}/../..)"
pkg_dir="${top_dir}/debworkroom"
taosx_dir="$(readlink -f ${script_dir}/../../../../taosx)"

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

libfile="libtaos.so"
pkg_libfile="libtaos.so.${tdengine_ver}"
nativelibfile="libtaosnative.so"
pkg_nativelibfile="libtaosnative.so.${tdengine_ver}"
wslibfile="libtaosws.so"
pkg_wslibfile="libtaosws.so.${tdengine_ver}"

# create install dir
install_home_path="/usr/local/taos"
mkdir -p ${pkg_dir}${install_home_path}
mkdir -p ${pkg_dir}${install_home_path}/bin
mkdir -p ${pkg_dir}${install_home_path}/cfg
#mkdir -p ${pkg_dir}${install_home_path}/connector
mkdir -p ${pkg_dir}${install_home_path}/driver
mkdir -p ${pkg_dir}${install_home_path}/examples
mkdir -p ${pkg_dir}${install_home_path}/include
#mkdir -p ${pkg_dir}${install_home_path}/init.d
mkdir -p ${pkg_dir}${install_home_path}/script

# # download taoskeeper and build
# if [ "$cpuType" = "x64" ] || [ "$cpuType" = "x86_64" ] || [ "$cpuType" = "amd64" ]; then
#   arch=amd64
# elif [ "$cpuType" = "x32" ] || [ "$cpuType" = "i386" ] || [ "$cpuType" = "i686" ]; then
#   arch=386
# elif [ "$cpuType" = "arm" ] || [ "$cpuType" = "aarch32" ]; then
#   arch=arm
# elif [ "$cpuType" = "arm64" ] || [ "$cpuType" = "aarch64" ]; then
#   arch=arm64
# else
#   arch=$cpuType
# fi

# echo "${top_dir}/../enterprise/packaging/build_taoskeeper.sh -r ${arch} -e taoskeeper -t ver-${tdengine_ver}"
# echo "$top_dir=${top_dir}"
# taoskeeper_binary=`${top_dir}/../enterprise/packaging/build_taoskeeper.sh -r $arch -e taoskeeper -t ver-${tdengine_ver}`
# echo "taoskeeper_binary: ${taoskeeper_binary}"

# copy config files
# cp $(dirname ${taoskeeper_binary})/config/taoskeeper.toml ${pkg_dir}${install_home_path}/cfg
# cp $(dirname ${taoskeeper_binary})/taoskeeper.service ${pkg_dir}${install_home_path}/cfg

cp ${compile_dir}/../packaging/cfg/taos.cfg         ${pkg_dir}${install_home_path}/cfg
cp ${compile_dir}/../packaging/cfg/taosd.service    ${pkg_dir}${install_home_path}/cfg

if [ -f "${compile_dir}/test/cfg/taosadapter.toml" ]; then
    cp ${compile_dir}/test/cfg/taosadapter.toml		${pkg_dir}${install_home_path}/cfg || :
fi
if [ -f "${compile_dir}/test/cfg/taosadapter.service" ]; then
    cp ${compile_dir}/test/cfg/taosadapter.service	${pkg_dir}${install_home_path}/cfg || :
fi
if [ -f "${compile_dir}/test/cfg/taoskeeper.toml" ]; then
    cp ${compile_dir}/test/cfg/taoskeeper.toml		${pkg_dir}${install_home_path}/cfg || :
fi
if [ -f "${compile_dir}/test/cfg/taoskeeper.service" ]; then
    cp ${compile_dir}/test/cfg/taoskeeper.service	${pkg_dir}${install_home_path}/cfg || :
fi
if [ -f "${taosx_dir}/explorer/server/examples/explorer.service" ]; then
    cp ${taosx_dir}/explorer/server/examples/explorer.service ${pkg_dir}${install_home_path}/cfg/taos-explorer.service || :
fi
if [ -f "${taosx_dir}/explorer/server/examples/explorer.toml" ]; then
    cp ${taosx_dir}/explorer/server/examples/explorer.toml	${pkg_dir}${install_home_path}/cfg || :
fi

# cp ${taoskeeper_binary}                      ${pkg_dir}${install_home_path}/bin
#cp ${compile_dir}/../packaging/deb/taosd            ${pkg_dir}${install_home_path}/init.d
cp ${compile_dir}/../packaging/tools/post.sh        ${pkg_dir}${install_home_path}/script
cp ${compile_dir}/../packaging/tools/preun.sh       ${pkg_dir}${install_home_path}/script
cp ${compile_dir}/../packaging/tools/startPre.sh    ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/../packaging/tools/set_core.sh    ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/../packaging/tools/taosd-dump-cfg.gdb    ${pkg_dir}${install_home_path}/bin
cp ${top_dir}/../enterprise/packaging/start-all.sh  ${pkg_dir}${install_home_path}/bin
cp ${top_dir}/../enterprise/packaging/stop-all.sh  ${pkg_dir}${install_home_path}/bin
sed -i "s/versionType=\"enterprise\"/versionType=\"community\"/g" ${pkg_dir}${install_home_path}/bin/start-all.sh
sed -i "s/versionType=\"enterprise\"/versionType=\"community\"/g" ${pkg_dir}${install_home_path}/bin/stop-all.sh


cp ${compile_dir}/build/bin/taosd                   ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/bin/taosudf                 ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/bin/taosBenchmark           ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/bin/taosdump               ${pkg_dir}${install_home_path}/bin

if [ -f "${compile_dir}/build/bin/taosadapter" ]; then
    cp ${compile_dir}/build/bin/taosadapter                    ${pkg_dir}${install_home_path}/bin ||:
fi
if [ -f "${compile_dir}/build/bin/taoskeeper" ]; then
    cp ${compile_dir}/build/bin/taoskeeper                    ${pkg_dir}${install_home_path}/bin ||:
fi

if [ -f "${taosx_dir}/target/release/taos-explorer" ]; then
    cp ${taosx_dir}/target/release/taos-explorer ${pkg_dir}${install_home_path}/bin ||:
fi

cp ${compile_dir}/build/bin/taos                    ${pkg_dir}${install_home_path}/bin
cp ${compile_dir}/build/lib/${libfile}              ${pkg_dir}${install_home_path}/driver/${pkg_libfile}     
cp ${compile_dir}/build/lib/${nativelibfile}        ${pkg_dir}${install_home_path}/driver/${pkg_nativelibfile}
cp ${compile_dir}/build/lib/${wslibfile}            ${pkg_dir}${install_home_path}/driver/${pkg_wslibfile} ||:
cp ${compile_dir}/../include/client/taos.h          ${pkg_dir}${install_home_path}/include
cp ${compile_dir}/../include/common/taosdef.h       ${pkg_dir}${install_home_path}/include
cp ${compile_dir}/../include/util/taoserror.h       ${pkg_dir}${install_home_path}/include
cp ${compile_dir}/../include/util/tdef.h            ${pkg_dir}${install_home_path}/include
cp ${compile_dir}/../include/libs/function/taosudf.h       ${pkg_dir}${install_home_path}/include
[ -f ${compile_dir}/build/include/taosws.h ] && cp ${compile_dir}/build/include/taosws.h            ${pkg_dir}${install_home_path}/include ||:
cp -r ${top_dir}/examples/*                         ${pkg_dir}${install_home_path}/examples
#cp -r ${top_dir}/src/connector/python               ${pkg_dir}${install_home_path}/connector
#cp -r ${top_dir}/src/connector/go                   ${pkg_dir}${install_home_path}/connector
#cp -r ${top_dir}/src/connector/nodejs               ${pkg_dir}${install_home_path}/connector
#cp ${compile_dir}/build/lib/taos-jdbcdriver*.*  ${pkg_dir}${install_home_path}/connector ||:

install_user_local_path="/usr/local"

if [ -f ${compile_dir}/build/bin/jemalloc-config ]; then
    mkdir -p ${pkg_dir}${install_user_local_path}/{bin,lib,lib/pkgconfig,include/jemalloc,share/doc/jemalloc,share/man/man3}
    cp ${compile_dir}/build/bin/jemalloc-config ${pkg_dir}${install_user_local_path}/bin/
    if [ -f ${compile_dir}/build/bin/jemalloc.sh ]; then
        cp ${compile_dir}/build/bin/jemalloc.sh ${pkg_dir}${install_user_local_path}/bin/
    fi
    if [ -f ${compile_dir}/build/bin/jeprof ]; then
        cp ${compile_dir}/build/bin/jeprof ${pkg_dir}${install_user_local_path}/bin/
    fi
    if [ -f ${compile_dir}/build/include/jemalloc/jemalloc.h ]; then
        cp ${compile_dir}/build/include/jemalloc/jemalloc.h ${pkg_dir}${install_user_local_path}/include/jemalloc/
    fi
    if [ -f ${compile_dir}/build/lib/libjemalloc.so.2 ]; then
        cp ${compile_dir}/build/lib/libjemalloc.so.2 ${pkg_dir}${install_user_local_path}/lib/
        ln -sf libjemalloc.so.2 ${pkg_dir}${install_user_local_path}/lib/libjemalloc.so
    fi
    # if [ -f ${compile_dir}/build/lib/libjemalloc.a ]; then
    #     cp ${compile_dir}/build/lib/libjemalloc.a ${pkg_dir}${install_user_local_path}/lib/
    # fi
    # if [ -f ${compile_dir}/build/lib/libjemalloc_pic.a ]; then
    #     cp ${compile_dir}/build/lib/libjemalloc_pic.a ${pkg_dir}${install_user_local_path}/lib/
    # fi
    if [ -f ${compile_dir}/build/lib/pkgconfig/jemalloc.pc ]; then
        cp ${compile_dir}/build/lib/pkgconfig/jemalloc.pc ${pkg_dir}${install_user_local_path}/lib/pkgconfig/
    fi
    if [ -f ${compile_dir}/build/share/doc/jemalloc/jemalloc.html ]; then
        cp ${compile_dir}/build/share/doc/jemalloc/jemalloc.html ${pkg_dir}${install_user_local_path}/share/doc/jemalloc/
    fi
    if [ -f ${compile_dir}/build/share/man/man3/jemalloc.3 ]; then
        cp ${compile_dir}/build/share/man/man3/jemalloc.3 ${pkg_dir}${install_user_local_path}/share/man/man3/
    fi
fi

cp -r ${compile_dir}/../packaging/deb/DEBIAN        ${pkg_dir}/
chmod 755 ${pkg_dir}/DEBIAN/*

# modify version of control
debver="Version: ${tdengine_ver}"
package="Package: ${product_name}"
sed -i "2c$debver" "${pkg_dir}/DEBIAN/control"
sed -i "2c$package" "${pkg_dir}/DEBIAN/control"

#get taos version, then set deb name
if [ "$verMode" == "cluster" ]; then
  debname="${product_name}-oss-${tdengine_ver}-${osType}-${cpuType}"
elif [ "$verMode" == "edge" ]; then
  debname="${product_name}-oss-${tdengine_ver}-${osType}-${cpuType}"
else
  echo "unknow verMode, nor cluster or edge"
  exit 1
fi

if [ "$verType" == "beta" ]; then
  debname="${product_name}-oss-${tdengine_ver}-${verType}-${osType}-${cpuType}.deb"
elif [ "$verType" == "stable" ]; then
  debname="${debname}.deb"
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

# rm -rf ${pkg_dir}/build-taoskeeper
# make deb package
dpkg -b ${pkg_dir} $debname
echo "make deb package success!"

cp ${pkg_dir}/*.deb ${output_dir}

# clean temp dir

rm -rf ${pkg_dir}
