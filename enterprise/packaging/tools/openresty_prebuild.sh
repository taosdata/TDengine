# /bin/bash
#
# Generate the deb/rpm package

set -e

. ./check_os.sh

# Current file structures.
curr_dir=$(pwd)
code_dir="$(readlink -m ${curr_dir}/..)"
top_dir="$(readlink -m ${code_dir}/..)"
build_dir="${top_dir}/build"
release_dir="${top_dir}/release"

mkdir -p ${build_dir}
cd ${curr_dir}
build_time=$(date +"%F %R")
install_dir=${code_dir}/thirdparty/openresty/binary/
git clone https://github.com/openresty/openresty-packaging.git 2> /dev/null || :

ZLIB_VER=1.2.11
PCRE_VER=8.41
SSL_VER=1.0.2k
OR_VER=1.13.6.1
if [ "$OS" == 'CentOS' ]; then
  # Make rpm file.
  mkdir -p ${install_dir}/rpms
  sudo yum install rpm-build redhat-rpm-config rpmdevtools
  sudo yum install openssl-devel zlib-devel pcre-devel gcc make perl \
  perl-Data-Dumper libtool ElectricFence systemtap-sdt-devel valgrind-devel
  sudo yum install yum-utils
  sudo yum-config-manager --add-repo https://openresty.org/package/centos/openresty.repo
  sudo yum install openresty-zlib-devel openresty-openssl-devel openresty-pcre-devel
  mkdir -p ${curr_dir}/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
  echo "%_topdir ${curr_dir}/rpmbuild" > ~/.rpmmacros
  cp ${curr_dir}/openresty-packaging/rpm/SOURCES/* ${curr_dir}/rpmbuild/SOURCES/
  cd ${curr_dir}/rpmbuild/SPECS
  cp ${curr_dir}/openresty-packaging/rpm/SPECS/*.spec ./
  for file in openresty-zlib.spec openresty-pcre.spec openresty-openssl.spec openresty.spec; do
    spectool -g -R $file
    rpmbuild -ba $file
  done
  cd ${curr_dir}/rpmbuild/RPMS/x86_64/.
  mv openresty-zlib-${ZLIB_VER}*.rpm ${install_dir}/rpms/.
  mv openresty-pcre-${PCRE_VER}*.rpm ${install_dir}/rpms/.
  mv openresty-openssl-${SSL_VER}*.rpm ${install_dir}/rpms/.
  mv openresty-${OR_VER}*.rpm ${install_dir}/rpms/.
  rm -rf ${curr_dir}/rpmbuild
elif [ "$OS" == 'Ubuntu' ]; then
  # Make deb file.
  mkdir -p ${install_dir}/debs
  wget -qO - https://openresty.org/package/pubkey.gpg | sudo apt-key add -
  sudo apt-get install libtemplate-perl dh-systemd systemtap-sdt-dev perl gnupg curl make build-essential dh-make bzr-builddeb
  sudo apt-get -y install software-properties-common
  # add the official openresty APT repository:
  sudo add-apt-repository -y "deb http://openresty.org/package/ubuntu $(lsb_release -sc) main"
  # to update the APT index:
  sudo apt-get update 2> /dev/null || :
  
  cd ${curr_dir}/openresty-packaging/deb
  make zlib-build OPTS="-uc -us"
  mv openresty-zlib_${ZLIB_VER}-*.deb ${install_dir}/debs/.
  make pcre-build OPTS="-uc -us"
  mv openresty-pcre_${PCRE_VER}*.deb ${install_dir}/debs/.
  #NOTE openssl maybe blocked by CHINA
  make openssl-build OPTS="-uc -us"
  mv openresty-openssl_${SSL_VER}*.deb ${install_dir}/debs/.
  make openresty-build OPTS="-uc -us"
  mv openresty_${OR_VER}*.deb ${install_dir}/debs/.
else 
  echo "not support this OS, Please contact the author"
fi

rm -rf ${curr_dir}/openresty-packaging

