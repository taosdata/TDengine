#!/bin/bash
. ./check_os.sh

build_time=$(date +"%F %R")
backup_time="$(echo ${build_time}| tr ': ' -)"
if [ -e "/usr/local/openresty/nginx/conf/nginx.conf" ]; then
    echo "Detected the OS have installed Openresty! Will backup to /usr/local/openresty_bak_tdengine!"
    sudo mv /usr/local/openresty /usr/local/openresty_bak_tdengine
fi

#localip=""
#if [ "$localip"x == "x" ]; then
#    LocalIP=`/sbin/ip addr | grep 'state UP' -A2 | awk '/inet/{print $2}'|cut -d/ -f1 |head -n1`
#else
#    LocalIP=$localip
#fi
#if [ "$LocalIP"x == "x" ]; then
#    LocalIP="127.0.0.1"
#fi
LocalIP="127.0.0.1"

#version automaticlly generated when release
ZLIB_VER=1.2.11
PCRE_VER=8.41
SSL_VER=1.0.2k
OR_VER=1.13.6.1
DISTRO=xenial
lib_dir="/usr/local/lib/tdengine"
link_dir="/usr/local/openresty"
install_openresty() {
    sudo systemctl stop openresty.service 2> /dev/null || :
    sudo systemctl disable openresty.service

    if [ "$OS" == 'CentOS' ]; then
        sudo rpm -e --nodeps openresty-zlib
        sudo rpm -ivh rpms/openresty-zlib-${ZLIB_VER}*.x86_64.rpm
        sudo rpm -e --nodeps openresty-pcre
        sudo rpm -ivh rpms/openresty-pcre-${PCRE_VER}*.x86_64.rpm
        sudo rpm -e --nodeps openresty-openssl
        sudo rpm -ivh rpms/openresty-openssl-${SSL_VER}*.el7.centos.x86_64.rpm
        sudo rpm -e --nodeps openresty
        sudo rpm -ivh rpms/openresty-${OR_VER}*.x86_64.rpm
    else
        sudo dpkg -P openresty
        sudo dpkg -i debs/openresty-zlib_${ZLIB_VER}*_amd64.deb
        sudo dpkg -i debs/openresty-pcre_${PCRE_VER}*_amd64.deb
        sudo dpkg -i debs/openresty-openssl_${SSL_VER}*_amd64.deb 
        sudo dpkg -i debs/openresty_${OR_VER}*_amd64.deb
    fi
    sudo cp conf/access.lua /usr/local/openresty/nginx/conf/.
    sudo cp conf/access_m.lua /usr/local/openresty/nginx/conf/.
    sudo cp conf/access_s.lua /usr/local/openresty/nginx/conf/.
    sudo cp conf/nginx.conf /usr/local/openresty/nginx/conf/.
    sudo cp conf/capture_sql.lua /usr/local/openresty/nginx/conf/.
    sudo cp conf/capture_grafana.lua /usr/local/openresty/nginx/conf/.
    sudo cp -rf lualib/resty /usr/local/openresty/lualib/.

    sudo mkdir -p ${link_dir}/web/.
    sudo ln -sf ${lib_dir}/web/admin ${link_dir}/web/.

    #rewrite nginx.conf
    sudo sed -i "s@10.0.2.15@${LocalIP}@g" /usr/local/openresty/nginx/conf/nginx.conf
    sudo sed -i "s@/home/zz/opt@${link_dir}@g" /usr/local/openresty/nginx/conf/nginx.conf
    sudo sed -i "s@^daemon off@#daemon off@g" /usr/local/openresty/nginx/conf/nginx.conf
    sudo sed -i "s@^master_process off@#master process off@g" /usr/local/openresty/nginx/conf/nginx.conf

    sudo systemctl enable openresty.service
    sudo systemctl restart openresty.service
}

## ==============================Main program starts from here============================
tar -zxf openresty.tar.gz

install_openresty

rm -rf $(tar -tf openresty.tar.gz)
