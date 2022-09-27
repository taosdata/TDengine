#!/bin/bash
#
# This file is used to install database on linux systems. The operating system
# is required to use systemd to manage services at boot

set -e
#set -x

verMode=edge
pagMode=full

iplist=""
serverFqdn=""

# -----------------------Variables definition---------------------
script_dir="../release"
# Dynamic directory
data_dir="/var/lib/taos"
log_dir="/var/log/taos"

data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"

cfg_install_dir="/etc/taos"

bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

#install main path
install_main_dir="/usr/local/taos"

# old bin dir
sbin_dir="/usr/local/taos/bin"

temp_version=""
fin_result=""

service_config_dir="/etc/systemd/system"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# =============================  get input parameters =================================================

# install.sh -v [server | client]  -e [yes | no] -i [systemd | service | ...]

# set parameters by default value
interactiveFqdn=yes   # [yes | no]
verType=server        # [server | client]
initType=systemd      # [systemd | service | ...]

while getopts "hv:d:" arg
do
  case $arg in
    d)
      #echo "interactiveFqdn=$OPTARG"
      script_dir=$( echo $OPTARG )
      ;;
    h)
      echo "Usage: `basename $0` -d scripy_path"
      exit 0
      ;;
    ?) #unknow option
      echo "unkonw argument"
      exit 1
      ;;
  esac
done

#echo "verType=${verType} interactiveFqdn=${interactiveFqdn}"

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function check_file() {
    #check file whether exists
    if [ ! -e $1/$2 ];then
        echo -e "$1/$2 \033[31mnot exists\033[0m!quit"
        fin_result=$fin_result"\033[31m$temp_version\033[0m test failed!\n"
        echo -e $fin_result
        exit 8
    fi
}

function get_package_name() {
  var=$1
	if [[ $1 =~ 'aarch' ]];then
		echo ${var::-21}
	else
		echo ${var::-17}
	fi
}

function check_link() {
    #check Link whether exists or broken
    if [ -L $1 ] ; then
        if [ ! -e $1 ] ; then
            echo -e "$1 \033[31Broken link\033[0m"
            fin_result=$fin_result"\033[31m$temp_version\033[0m test failed!\n"
            echo -e $fin_result
            exit 8
        fi
    else
        echo -e "$1 \033[31mnot exists\033[0m!quit"
        fin_result=$fin_result"\033[31m$temp_version\033[0m test failed!\n"
        echo -e $fin_result
        exit 8
    fi
}

function check_main_path() {
    #check install main dir and all sub dir
    main_dir=("" "cfg" "bin" "connector" "driver" "examples" "include" "init.d")
    for i in "${main_dir[@]}";do
        check_file ${install_main_dir} $i
    done
    if [ "$verMode" == "cluster" ]; then
        check_file ${install_main_dir} "share/admin"
    fi
    echo -e "Check main path:\033[32mOK\033[0m!"
}

function check_bin_path() {
    # check install bin dir and all sub dir
    bin_dir=("taos" "taosd" "taosadapter" "taosdemo" "remove.sh" "tarbitrator" "set_core.sh")
    for i in "${bin_dir[@]}";do
        check_file ${sbin_dir} $i
    done
    lbin_dir=("taos" "taosd" "taosadapter" "taosdemo" "rmtaos" "tarbitrator" "set_core")
    for i in "${lbin_dir[@]}";do
        check_link ${bin_link_dir}/$i
    done
    echo -e "Check bin  path:\033[32mOK\033[0m!"
}

function check_lib_path() {
    # check all links
    check_link ${lib_link_dir}/libtaos.so
    check_link ${lib_link_dir}/libtaos.so.1

    if [[ -d ${lib64_link_dir}  ]]; then
        check_link ${lib64_link_dir}/libtaos.so
        check_link ${lib64_link_dir}/libtaos.so.1
    fi
    echo -e "Check lib  path:\033[32mOK\033[0m!"
}

function check_header_path() {
	# check all header
	header_dir=("taos.h" "taosdef.h" "taoserror.h" "taosudf.h")
    for i in "${header_dir[@]}";do
        check_link ${inc_link_dir}/$i
    done
    echo -e "Check bin  path:\033[32mOK\033[0m!"
}

function check_taosadapter_config_dir() {
	# check all config
	check_file ${cfg_install_dir} taosadapter.toml
	check_file ${cfg_install_dir} taosadapter.service
	check_file ${install_main_dir}/cfg taosadapter.toml.org
	echo -e "Check conf path:\033[32mOK\033[0m!"
}

function check_config_dir() {
	# check all config
	check_file ${cfg_install_dir} taos.cfg
	check_file ${install_main_dir}/cfg taos.cfg.org
	echo -e "Check conf path:\033[32mOK\033[0m!"
}

function check_log_path() {
	# check log path
	check_file ${log_dir}
	echo -e "Check log  path:\033[32mOK\033[0m!"
}

function check_data_path() {
	# check data path
	check_file ${data_dir}
	echo -e "Check data path:\033[32mOK\033[0m!"
}

function install_TDengine() {
	cd ${script_dir}
	tar zxf $1
  temp_version=$(get_package_name $1)
	cd $(get_package_name $1)
  echo -e "\033[32muninstall TDengine && install TDengine...\033[0m"
	rmtaos >/dev/null 2>&1 || echo 'taosd not installed' && echo -e '\n\n' |./install.sh >/dev/null 2>&1
  echo -e "\033[32mTDengine has been installed!\033[0m"
  echo -e "\033[32mTDengine is starting...\033[0m"
  kill_process taos && systemctl start taosd && sleep 10
}

function test_TDengine() {
    check_main_path
    check_bin_path
    check_lib_path
    check_header_path
    check_config_dir
    check_taosadapter_config_dir
    check_log_path
    check_data_path
    result=`taos -s 'create database test ;create table test.tt(ts timestamp ,i int);insert into test.tt values(now,11);select * from test.tt' 2>&1 ||:`
    if [[ $result =~ "Unable to establish" ]];then
        echo -e "\033[31mTDengine connect failed\033[0m"
        fin_result=$fin_result"\033[31m$temp_version\033[0m test failed!\n"
        echo -e $fin_result
        exit 8
    fi
    echo -e "Check TDengine connect:\033[32mOK\033[0m!"
    fin_result=$fin_result"\033[32m$temp_version\033[0m test OK!\n"
}
# ## ==============================Main program starts from here============================
TD_package_name=`ls ${script_dir}/*server*gz |awk -F '/' '{print $NF}' `
temp=`pwd`
for i in $TD_package_name;do
	if [[ $i =~ 'enterprise' ]];then
		verMode="cluster"
	else
		verMode=""
	fi
  cd $temp
	install_TDengine $i
	test_TDengine
done
echo "============================================================"
echo -e $fin_result
