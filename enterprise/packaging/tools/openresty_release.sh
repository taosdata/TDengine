# /bin/bash
#
# tar the deb/rpm package

set -e

. ./check_os.sh

# Current file structures.
curr_dir=$(pwd)
code_dir="$(readlink -m ${curr_dir}/..)"
top_dir="$(readlink -m ${code_dir}/..)"
build_dir="${top_dir}/build"
release_dir="${top_dir}/release"
binary_dir=${code_dir}/thirdparty/openresty/binary/

mkdir -p ${build_dir}
cd ${curr_dir}
build_time=$(date +"%F %R")
install_dir="openresty-$(echo ${build_time}| tr ': ' -)-${USER}"

ZLIB_VER=1.2.11
PCRE_VER=8.41
SSL_VER=1.0.2k
OR_VER=1.13.6.1
# Make rpm file.
mkdir -p ${install_dir}/rpms
cp ${binary_dir}/rpms/openresty-zlib-${ZLIB_VER}*.rpm ${curr_dir}/${install_dir}/rpms/.
cp ${binary_dir}/rpms/openresty-pcre-${PCRE_VER}*.rpm ${curr_dir}/${install_dir}/rpms/.
cp ${binary_dir}/rpms/openresty-openssl-${SSL_VER}*.rpm ${curr_dir}/${install_dir}/rpms/.
cp ${binary_dir}/rpms/openresty-${OR_VER}*.rpm ${curr_dir}/${install_dir}/rpms/.
# Make deb file.
mkdir -p ${install_dir}/debs
cp ${binary_dir}/debs/openresty-zlib_${ZLIB_VER}-*.deb ${curr_dir}/${install_dir}/debs/.
cp ${binary_dir}/debs/openresty-pcre_${PCRE_VER}*.deb ${curr_dir}/${install_dir}/debs/.
cp ${binary_dir}/debs/openresty-openssl_${SSL_VER}*.deb ${curr_dir}/${install_dir}/debs/.
cp ${binary_dir}/debs/openresty_${OR_VER}*.deb ${curr_dir}/${install_dir}/debs/.

cd ${curr_dir}/${install_dir}
tar -zcv -f openresty.tar.gz * --remove-files 
mv "openresty.tar.gz" ${build_dir}/.
cd ${build_dir}
rm -rf ${curr_dir}/${install_dir}

