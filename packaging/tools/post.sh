#!/bin/bash
#
# This file is used to install tdengine rpm package on centos systems. The operating system
# is required to use systemd to manage services at boot
# set -x

iplist=""
serverFqdn=""

osType=`uname`

# Dynamic directory
data_dir="/var/lib/taos"
log_dir="/var/log/taos"
cfg_install_dir="/etc/taos"

if [ "$osType" != "Darwin" ]; then
  script_dir=$(dirname $(readlink -f "$0"))
  verNumber=""
  lib_file_ext="so"
  lib_file_ext_1="so.1"

  bin_link_dir="/usr/bin"
  lib_link_dir="/usr/lib"
  lib64_link_dir="/usr/lib64"
  inc_link_dir="/usr/include"
  
  install_main_dir="/usr/local/taos"
else
  script_dir=${source_dir}/packaging/tools
  verNumber=`ls tdengine/driver | grep -E "libtaos\.[0-9]\.[0-9]" | sed "s/libtaos.//g" |  sed "s/.dylib//g" | head -n 1`
  lib_file_ext="dylib"
  lib_file_ext_1="1.dylib"

  bin_link_dir="/usr/local/bin"
  lib_link_dir="/usr/local/lib"
  lib64_link_dir="/usr/local/lib"
  inc_link_dir="/usr/local/include"

  if [ -d "/usr/local/Cellar/" ];then
    install_main_dir="/usr/local/Cellar/tdengine/${verNumber}"
  elif [ -d "/opt/homebrew/Cellar/" ];then
    install_main_dir="/opt/homebrew/Cellar/tdengine/${verNumber}"
  else
    install_main_dir="/usr/local/taos"
  fi
fi

data_link_dir="${install_main_dir}/data"
log_link_dir="${install_main_dir}/log"
install_log_path="${log_dir}/tdengine_install.log"

# static directory
cfg_dir="${install_main_dir}/cfg"
bin_dir="${install_main_dir}/bin"
lib_dir="${install_main_dir}/driver"
init_d_dir="${install_main_dir}/init.d"
inc_dir="${install_main_dir}/include"

service_config_dir="/etc/systemd/system"


# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
csudouser=""
if command -v sudo > /dev/null; then
    csudo="sudo "
    csudouser="sudo -u ${USER} "
fi

initd_mod=0
service_mod=2
if ps aux | grep -v grep | grep systemd &> /dev/null; then
    service_mod=0
elif $(which service &> /dev/null); then
    service_mod=1
    service_config_dir="/etc/init.d"
    if $(which chkconfig &> /dev/null); then
         initd_mod=1
    elif $(which insserv &> /dev/null); then
        initd_mod=2
    elif $(which update-rc.d &> /dev/null); then
        initd_mod=3
    else
        service_mod=2
    fi
else
    service_mod=2
fi
if [ "$osType" = "Darwin" ]; then
  if [ -d "${install_main_dir}" ];then
    ${csudo}rm -rf ${install_main_dir}
  fi
  ${csudo}mkdir -p ${install_main_dir}
  ${csudo}rm -rf ${install_main_dir}
  ${csudo}cp -rf tdengine ${install_main_dir}
fi

function log_print(){
  now=$(date +"%D %T")
  echo "$now  $1" >> ${install_log_path}
}

function kill_taosadapter() {
#  ${csudo}pkill -f taosadapter || :
  pid=$(ps -ef | grep "taosadapter" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function kill_taoskeeper() {
  pid=$(ps -ef | grep "taoskeeper" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function kill_taosd() {
#  ${csudo}pkill -f taosd || :
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function install_include() {
    log_print "start install include from ${inc_dir} to ${inc_link_dir}"
    ${csudo}mkdir -p ${inc_link_dir}
    ${csudo}rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taosdef.h ${inc_link_dir}/taoserror.h ${inc_link_dir}/tdef.h ${inc_link_dir}/taosudf.h || :
    [ -f ${inc_link_dir}/taosws.h ] && ${csudo}rm -f ${inc_link_dir}/taosws.h ||:

    ${csudo}ln -s ${inc_dir}/taos.h ${inc_link_dir}/taos.h
    ${csudo}ln -s ${inc_dir}/taosdef.h ${inc_link_dir}/taosdef.h
    ${csudo}ln -s ${inc_dir}/taoserror.h ${inc_link_dir}/taoserror.h
    ${csudo}ln -s ${inc_dir}/tdef.h ${inc_link_dir}/tdef.h
    ${csudo}ln -s ${inc_dir}/taosudf.h ${inc_link_dir}/taosudf.h

    [ -f ${inc_dir}/taosws.h ] && ${csudo}ln -sf ${inc_dir}/taosws.h ${inc_link_dir}/taosws.h ||:
    log_print "install include success"
}

function install_jemalloc() {
  jemalloc_dir=${script_dir}/../jemalloc

  if [ -d ${jemalloc_dir} ]; then
    ${csudo}/usr/bin/install -c -d /usr/local/bin

    if [ -f ${jemalloc_dir}/bin/jemalloc-config ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc-config /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jemalloc.sh ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jemalloc.sh /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/bin/jeprof ]; then
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/bin/jeprof /usr/local/bin
    fi
    if [ -f ${jemalloc_dir}/include/jemalloc/jemalloc.h ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/include/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/include/jemalloc/jemalloc.h /usr/local/include/jemalloc
    fi
    if [ -f ${jemalloc_dir}/lib/libjemalloc.so.2 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.so.2 /usr/local/lib
      ${csudo}ln -sf libjemalloc.so.2 /usr/local/lib/libjemalloc.so
      ${csudo}/usr/bin/install -c -d /usr/local/lib
      # if [ -f ${jemalloc_dir}/lib/libjemalloc.a ]; then
      #   ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc.a /usr/local/lib
      # fi
      # if [ -f ${jemalloc_dir}/lib/libjemalloc_pic.a ]; then
      #   ${csudo}/usr/bin/install -c -m 755 ${jemalloc_dir}/lib/libjemalloc_pic.a /usr/local/lib
      # fi
      if [ -f ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc ]; then
        ${csudo}/usr/bin/install -c -d /usr/local/lib/pkgconfig
        ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/lib/pkgconfig/jemalloc.pc /usr/local/lib/pkgconfig
      fi
    fi
    if [ -f ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/doc/jemalloc
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/share/doc/jemalloc/jemalloc.html /usr/local/share/doc/jemalloc
    fi
    if [ -f ${jemalloc_dir}/share/man/man3/jemalloc.3 ]; then
      ${csudo}/usr/bin/install -c -d /usr/local/share/man/man3
      ${csudo}/usr/bin/install -c -m 644 ${jemalloc_dir}/share/man/man3/jemalloc.3 /usr/local/share/man/man3
    fi

    if [ -d /etc/ld.so.conf.d ]; then
      echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/jemalloc.conf >/dev/null || echo -e "failed to write /etc/ld.so.conf.d/jemalloc.conf"
      ${csudo}ldconfig
    else
      echo "/etc/ld.so.conf.d not found!"
    fi
  fi
}

function install_lib() {
    log_print "start install lib from ${lib_dir} to ${lib_link_dir}"
    ${csudo}rm -f ${lib_link_dir}/libtaos* || :
    ${csudo}rm -f ${lib64_link_dir}/libtaos* || :

    [ -f ${lib_link_dir}/libtaosws.${lib_file_ext} ] && ${csudo}rm -f ${lib_link_dir}/libtaosws.${lib_file_ext} || :
    [ -f ${lib64_link_dir}/libtaosws.${lib_file_ext} ] && ${csudo}rm -f ${lib64_link_dir}/libtaosws.${lib_file_ext} || :

    ${csudo}ln -s ${lib_dir}/libtaos.* ${lib_link_dir}/libtaos.${lib_file_ext_1} 2>>${install_log_path} || return 1
    ${csudo}ln -s ${lib_link_dir}/libtaos.${lib_file_ext_1} ${lib_link_dir}/libtaos.${lib_file_ext} 2>>${install_log_path} || return 1

    [ -f ${lib_dir}/libtaosws.${lib_file_ext} ] && ${csudo}ln -sf ${lib_dir}/libtaosws.${lib_file_ext} ${lib_link_dir}/libtaosws.${lib_file_ext} ||:

    if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.${lib_file_ext} ]]; then
      ${csudo}ln -s ${lib_dir}/libtaos.* ${lib64_link_dir}/libtaos.${lib_file_ext_1} 2>>${install_log_path} || return 1
      ${csudo}ln -s ${lib64_link_dir}/libtaos.${lib_file_ext_1} ${lib64_link_dir}/libtaos.${lib_file_ext}  2>>${install_log_path} || return 1

      [ -f ${lib_dir}/libtaosws.${lib_file_ext} ] && ${csudo}ln -sf ${lib_dir}/libtaosws.${lib_file_ext} ${lib64_link_dir}/libtaosws.${lib_file_ext} 2>>${install_log_path}
    fi

    if [ "$osType" != "Darwin" ]; then
      ${csudo}ldconfig
    fi

    log_print "install lib success"
}

function install_bin() {
    # Remove links
    log_print "start install bin from ${bin_dir} to ${bin_link_dir}"
    ${csudo}rm -f ${bin_link_dir}/taos     || :
    ${csudo}rm -f ${bin_link_dir}/taosd    || :
    ${csudo}rm -f ${bin_link_dir}/udfd     || :
    ${csudo}rm -f ${bin_link_dir}/taosadapter     || :
    ${csudo}rm -f ${bin_link_dir}/taosBenchmark || :
    ${csudo}rm -f ${bin_link_dir}/taoskeeper || :
    ${csudo}rm -f ${bin_link_dir}/taosdemo || :
    ${csudo}rm -f ${bin_link_dir}/taosdump || :
    ${csudo}rm -f ${bin_link_dir}/rmtaos   || :
    ${csudo}rm -f ${bin_link_dir}/set_core || :
    ${csudo}rm -f ${bin_link_dir}/*explorer || :

    ${csudo}chmod 0555 ${bin_dir}/*

    #Make link
    if [ -x ${bin_dir}/taos ]; then
      ${csudo}ln -s ${bin_dir}/taos ${bin_link_dir}/taos                     2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/taosd ]; then
      ${csudo}ln -s ${bin_dir}/taosd ${bin_link_dir}/taosd                   2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/udfd ]; then
      ${csudo}ln -s ${bin_dir}/udfd ${bin_link_dir}/udfd                     2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/taosadapter ]; then
      ${csudo}ln -s ${bin_dir}/taosadapter ${bin_link_dir}/taosadapter       2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/taosBenchmark ]; then
      ${csudo}ln -sf ${bin_dir}/taosBenchmark ${bin_link_dir}/taosdemo       2>>${install_log_path}
      ${csudo}ln -sf ${bin_dir}/taosBenchmark ${bin_link_dir}/taosBenchmark  2>>${install_log_path}
    fi
    if [ -x ${bin_dir}/TDinsight.sh ]; then
      ${csudo}ln -sf ${bin_dir}/TDinsight.sh ${bin_link_dir}/TDinsight.sh    2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/taosdump ]; then
      ${csudo}ln -s ${bin_dir}/taosdump ${bin_link_dir}/taosdump             2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/set_core.sh ]; then
      ${csudo}ln -s ${bin_dir}/set_core.sh ${bin_link_dir}/set_core          2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/remove.sh ]; then
      ${csudo}ln -s ${bin_dir}/remove.sh ${bin_link_dir}/rmtaos              2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/taoskeeper ]; then
      ${csudo}ln -sf ${bin_dir}/taoskeeper ${bin_link_dir}/taoskeeper        2>>${install_log_path} || return 1
    fi
    if [ -x ${bin_dir}/*explorer ]; then
      ${csudo}ln -s ${bin_dir}/*explorer ${bin_link_dir}/*explorer           2>>${install_log_path} || return 1
    fi
    log_print "install bin success"
}

function add_newHostname_to_hosts() {
  localIp="127.0.0.1"
  OLD_IFS="$IFS"
  IFS=" "
  iphost=$(cat /etc/hosts | grep $1 | awk '{print $1}')
  arr=($iphost)
  IFS="$OLD_IFS"
  for s in "${arr[@]}"
  do
    if [[ "$s" == "$localIp" ]]; then
      return
    fi
  done
  ${csudo}echo "127.0.0.1  $1" >> /etc/hosts   ||:
}

function set_hostname() {
  echo -e -n "${GREEN}Please enter one hostname(must not be 'localhost')${NC}:"
  read newHostname
  while true; do
    if [[ ! -z "$newHostname" && "$newHostname" != "localhost" ]]; then
      break
    else
      read -p "Please enter one hostname(must not be 'localhost'):" newHostname
      break
    fi
  done

  ${csudo}hostname $newHostname ||:
  retval=`echo $?`
  if [[ $retval != 0 ]]; then
   echo
   echo "set hostname fail!"
   return
  fi
  #echo -e -n "$(hostnamectl status --static)"
  #echo -e -n "$(hostnamectl status --transient)"
  #echo -e -n "$(hostnamectl status --pretty)"

  #ubuntu/centos /etc/hostname
  if [[ -e /etc/hostname ]]; then
    ${csudo}echo $newHostname > /etc/hostname   ||:
  fi

  #debian: #HOSTNAME=yourname
  if [[ -e /etc/sysconfig/network ]]; then
    ${csudo}sed -i -r "s/#*\s*(HOSTNAME=\s*).*/\1$newHostname/" /etc/sysconfig/network   ||:
  fi

  ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${cfg_install_dir}/taos.cfg
  serverFqdn=$newHostname

  if [[ -e /etc/hosts ]]; then
    add_newHostname_to_hosts $newHostname
  fi
}

function is_correct_ipaddr() {
  newIp=$1
  OLD_IFS="$IFS"
  IFS=" "
  arr=($iplist)
  IFS="$OLD_IFS"
  for s in "${arr[@]}"
  do
   if [[ "$s" == "$newIp" ]]; then
     return 0
   fi
  done

  return 1
}

function set_ipAsFqdn() {
  iplist=$(ip address |grep inet |grep -v inet6 |grep -v 127.0.0.1 |awk '{print $2}' |awk -F "/" '{print $1}') ||:
  if [ -z "$iplist" ]; then
    iplist=$(ifconfig |grep inet |grep -v inet6 |grep -v 127.0.0.1 |awk '{print $2}' |awk -F ":" '{print $2}') ||:
  fi

  if [ -z "$iplist" ]; then
    echo
    echo -e -n "${GREEN}Unable to get local ip, use 127.0.0.1${NC}"
    localFqdn="127.0.0.1"
    # Write the local FQDN to configuration file
    ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/taos.cfg
    serverFqdn=$localFqdn
    echo
    return
  fi

  echo -e -n "${GREEN}Please choose an IP from local IP list${NC}:"
  echo
  echo -e -n "${GREEN}$iplist${NC}"
  echo
  echo
  echo -e -n "${GREEN}Notes: if IP is used as the node name, data can NOT be migrated to other machine directly${NC}:"
  read localFqdn
    while true; do
      if [ ! -z "$localFqdn" ]; then
        # Check if correct ip address
        is_correct_ipaddr $localFqdn
        retval=`echo $?`
        if [[ $retval != 0 ]]; then
          read -p "Please choose an IP from local IP list:" localFqdn
        else
          # Write the local FQDN to configuration file
          ${csudo}sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/taos.cfg
          serverFqdn=$localFqdn
          break
        fi
      else
        read -p "Please choose an IP from local IP list:" localFqdn
      fi
    done
}

function local_fqdn_check() {
  #serverFqdn=$(hostname)
  echo
  echo -e -n "System hostname is: ${GREEN}$serverFqdn${NC}"
  echo
  if [[ "$serverFqdn" == "" ]] || [[ "$serverFqdn" == "localhost"  ]]; then
    echo -e -n "${GREEN}It is strongly recommended to configure a hostname for this machine ${NC}"
    echo

    while true
    do
        read -r -p "Set hostname now? [Y/n] " input
        if [ ! -n "$input" ]; then
            set_hostname
            break
        else
            case $input in
            [yY][eE][sS]|[yY])
                set_hostname
                break
                ;;

            [nN][oO]|[nN])
                set_ipAsFqdn
                break
                ;;

            *)
                set_ipAsFqdn
                echo "Invalid input..."
                break
                ;;
            esac
        fi
    done
  fi
}

function install_taosadapter_config() {
    if [ ! -f "${cfg_install_dir}/taosadapter.toml" ]; then
        [ ! -d ${cfg_install_dir} ] &&
            ${csudo}${csudo}mkdir -p ${cfg_install_dir}
        [ -f ${cfg_dir}/taosadapter.toml ] && ${csudo}cp ${cfg_dir}/taosadapter.toml ${cfg_install_dir}
        [ -f ${cfg_install_dir}/taosadapter.toml ] &&
            ${csudo}chmod 644 ${cfg_install_dir}/taosadapter.toml
    fi

    [ -f ${cfg_dir}/taosadapter.toml ] &&
        ${csudo}mv ${cfg_dir}/taosadapter.toml ${cfg_dir}/taosadapter.toml.new

    [ -f ${cfg_install_dir}/taosadapter.toml ] &&
        ${csudo}ln -s ${cfg_install_dir}/taosadapter.toml ${cfg_dir}
}

function install_taoskeeper_config() {
    # if new environment without taoskeeper
    if [[ ! -f "${cfg_install_dir}/keeper.toml" ]] && [[ ! -f "${cfg_install_dir}/taoskeeper.toml" ]]; then
        [ ! -d ${cfg_install_dir} ] && ${csudo}${csudo}mkdir -p ${cfg_install_dir}
        [ -f ${cfg_dir}/taoskeeper.toml ] && ${csudo}cp ${cfg_dir}/taoskeeper.toml ${cfg_install_dir}
        [ -f ${cfg_install_dir}/taoskeeper.toml ] &&
            ${csudo}chmod 644 ${cfg_install_dir}/taoskeeper.toml
    fi
    # if old machine with taoskeeper.toml file
    if [ -f ${cfg_install_dir}/taoskeeper.toml ]; then
        ${csudo}mv ${cfg_dir}/taoskeeper.toml ${cfg_dir}/taoskeeper.toml.new
    fi

    if [ -f ${cfg_install_dir}/keeper.toml ]; then
        echo "The file keeper.toml will be renamed to taoskeeper.toml"
        ${csudo}mv ${cfg_install_dir}/keeper.toml ${cfg_install_dir}/taoskeeper.toml
        ${csudo}mv ${cfg_dir}/taoskeeper.toml ${cfg_dir}/taoskeeper.toml.new
    fi

    [ -f ${cfg_install_dir}/taoskeeper.toml ] &&
        ${csudo}ln -s ${cfg_install_dir}/taoskeeper.toml ${cfg_dir}
}

function install_config() {
    log_print "start install config from ${cfg_dir} to ${cfg_install_dir}"
    if [ ! -f "${cfg_install_dir}/taos.cfg" ]; then
        ${csudo}${csudo}mkdir -p ${cfg_install_dir}
        [ -f ${cfg_dir}/taos.cfg ] && ${csudo}cp ${cfg_dir}/taos.cfg ${cfg_install_dir}
        ${csudo}chmod 644 ${cfg_install_dir}/*
    fi

    # Save standard input to 6 and open / dev / TTY on standard input
    exec 6<&0 0</dev/tty

    local_fqdn_check

    # restore the backup standard input, and turn off 6
    exec 0<&6 6<&-

    ${csudo}mv ${cfg_dir}/taos.cfg ${cfg_dir}/taos.cfg.new
    ${csudo}ln -s ${cfg_install_dir}/taos.cfg ${cfg_dir}
    #FQDN_FORMAT="(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
    #FQDN_FORMAT="(:[1-6][0-9][0-9][0-9][0-9]$)"
    #PORT_FORMAT="(/[1-6][0-9][0-9][0-9][0-9]?/)"
    #FQDN_PATTERN=":[0-9]{1,5}$"

    # first full-qualified domain name (FQDN) for TDengine cluster system
    echo
    echo -e -n "${GREEN}Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join${NC}"
    echo
    echo -e -n "${GREEN}OR leave it blank to build one${NC}:"
    #read firstEp
    if exec < /dev/tty; then
        read firstEp;
    fi
    while true; do
    if [ ! -z "$firstEp" ]; then
        # check the format of the firstEp
        #if [[ $firstEp == $FQDN_PATTERN ]]; then
            # Write the first FQDN to configuration file
            ${csudo}sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${cfg_install_dir}/taos.cfg
            break
        #else
        #    read -p "Please enter the correct FQDN:port: " firstEp
        #fi
    else
        break
    fi
  done

    # user email
    #EMAIL_PATTERN='^[A-Za-z0-9\u4e00-\u9fa5]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$'
    #EMAIL_PATTERN='^[\w-]+(\.[\w-]+)*@[\w-]+(\.[\w-]+)+$'
    #EMAIL_PATTERN="^[\w-]+(\.[\w-]+)*@[\w-]+(\.[\w-]+)+$"
    echo
    echo -e -n "${GREEN}Enter your email address for priority support or enter empty to skip${NC}: "
    read emailAddr
    while true; do
        if [ ! -z "$emailAddr" ]; then
            # check the format of the emailAddr
            #if [[ "$emailAddr" =~ $EMAIL_PATTERN ]]; then
                # Write the email address to temp file
                email_file="${install_main_dir}/email"
                ${csudo}bash -c "echo $emailAddr > ${email_file}"
                break
            #else
            #    read -p "Please enter the correct email address: " emailAddr
            #fi
        else
            break
        fi
    done

    log_print "install config success"
}

function clean_service_on_sysvinit() {
    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo}sed -i "\|${restart_config_str}|d" /etc/inittab || :

    if ps aux | grep -v grep | grep taosd &> /dev/null; then
        ${csudo}service taosd stop || :
    fi

    if ((${initd_mod}==1)); then
        ${csudo}chkconfig --del taosd || :
    elif ((${initd_mod}==2)); then
        ${csudo}insserv -r taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo}update-rc.d -f taosd remove || :
    fi

    ${csudo}rm -f ${service_config_dir}/taosd || :

    if $(which init &> /dev/null); then
        ${csudo}init q || :
    fi
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service
    ${csudo}cp %{init_d_dir}/taosd ${service_config_dir} && ${csudo}chmod a+x ${service_config_dir}/taosd

    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo}grep -q -F "$restart_config_str" /etc/inittab || ${csudo}bash -c "echo '${restart_config_str}' >> /etc/inittab"

    if ((${initd_mod}==1)); then
        ${csudo}chkconfig --add taosd || :
        ${csudo}chkconfig --level 2345 taosd on || :
    elif ((${initd_mod}==2)); then
        ${csudo}insserv taosd || :
        ${csudo}insserv -d taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo}update-rc.d taosd defaults || :
    fi
}

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/taosd.service"

    # taosd service already is stopped before install in preinst script
    #if systemctl is-active --quiet taosd; then
    #    echo "TDengine is running, stopping it..."
    #    ${csudo}systemctl stop taosd &> /dev/null || echo &> /dev/null
    #fi
    ${csudo}systemctl disable taosd &> /dev/null || echo &> /dev/null

    ${csudo}rm -f ${taosd_service_config}
}

# taos:2345:respawn:/etc/init.d/taosd start

function install_service_on_systemd() {
    clean_service_on_systemd

    taosd_service_config="${service_config_dir}/taosd.service"

    ${csudo}bash -c "echo '[Unit]'                             >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'Description=TDengine server service' >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'After=network-online.target'        >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'Wants=network-online.target'        >> ${taosd_service_config}"
    ${csudo}bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo}bash -c "echo '[Service]'                          >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'Type=simple'                        >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'ExecStart=/usr/bin/taosd'           >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'ExecStartPre=/usr/local/taos/bin/startPre.sh'           >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'TimeoutStopSec=1000000s'            >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'LimitNOFILE=infinity'               >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'LimitNPROC=infinity'                >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'LimitCORE=infinity'                 >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'TimeoutStartSec=0'                  >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'StandardOutput=null'                >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'Restart=always'                     >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'StartLimitBurst=3'                  >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'StartLimitInterval=60s'             >> ${taosd_service_config}"
    ${csudo}bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo}bash -c "echo '[Install]'                          >> ${taosd_service_config}"
    ${csudo}bash -c "echo 'WantedBy=multi-user.target'         >> ${taosd_service_config}"
    ${csudo}systemctl enable taosd
}

function install_service_on_launchctl() {
  if [ -f ${install_main_dir}/service/com.taosdata.taosd.plist ]; then
    ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosd.plist > /dev/null 2>&1 || :
    ${csudo}cp ${install_main_dir}/service/com.taosdata.taosd.plist /Library/LaunchDaemons/com.taosdata.taosd.plist || :
    ${csudo}launchctl load -w /Library/LaunchDaemons/com.taosdata.taosd.plist || :
  fi
  if [ -f ${install_main_dir}/service/com.taosdata.taosadapter.plist ]; then
    ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taosadapter.plist > /dev/null 2>&1 || :
    ${csudo}cp ${install_main_dir}/service/com.taosdata.taosadapter.plist /Library/LaunchDaemons/com.taosdata.taosadapter.plist || :
    ${csudo}launchctl load -w /Library/LaunchDaemons/com.taosdata.taosadapter.plist || :
  fi
  if [ -f ${install_main_dir}/service/com.taosdata.taoskeeper.plist ]; then
    ${csudo}launchctl unload -w /Library/LaunchDaemons/com.taosdata.taoskeeper.plist > /dev/null 2>&1 || :
    ${csudo}cp ${install_main_dir}/service/com.taosdata.taoskeeper.plist /Library/LaunchDaemons/com.taosdata.taoskeeper.plist || :
    ${csudo}launchctl load -w /Library/LaunchDaemons/com.taosdata.taoskeeper.plist || :
  fi
}

function install_taosadapter_service() {
    if ((${service_mod}==0)); then
        [ -f ${script_dir}/../cfg/taosadapter.service ] &&\
            ${csudo}cp ${script_dir}/../cfg/taosadapter.service \
            ${service_config_dir}/ || :
        ${csudo}systemctl daemon-reload
    fi
}

function install_taoskeeper_service() {
    if ((${service_mod}==0)); then
        [ -f ${script_dir}/../cfg/taoskeeper.service ] &&\
            ${csudo}cp ${script_dir}/../cfg/taoskeeper.service \
            ${service_config_dir}/ || :
        ${csudo}systemctl daemon-reload
    fi
}

function install_service() {
  log_print "start install service"
  if [ "$osType" != "Darwin" ]; then
    if ((${service_mod}==0)); then
        install_service_on_systemd
    elif ((${service_mod}==1)); then
        install_service_on_sysvinit
    else
        # manual start taosd
        kill_taosadapter
        kill_taosd
    fi
  else
    install_service_on_launchctl
  fi
  log_print "install service success"
}

function install_app() {
  if [ "$osType" = "Darwin" ]; then
    if [ -f ${install_main_dir}/service/TDengine ]; then
      ${csudo}rm -rf /Applications/TDengine.app &&
        ${csudo}mkdir -p /Applications/TDengine.app/Contents/MacOS/ &&
        ${csudo}cp ${install_main_dir}/service/TDengine /Applications/TDengine.app/Contents/MacOS/ &&
        echo "<plist><dict></dict></plist>" | ${csudo}tee /Applications/TDengine.app/Contents/Info.plist > /dev/null &&
        ${csudo}sips -i ${install_main_dir}/service/logo.png > /dev/null && 
        DeRez -only icns ${install_main_dir}/service/logo.png | ${csudo}tee /Applications/TDengine.app/mac_logo.rsrc > /dev/null &&
        ${csudo}rez -append /Applications/TDengine.app/mac_logo.rsrc -o $'/Applications/TDengine.app/Icon\r' &&
        ${csudo}SetFile -a C /Applications/TDengine.app/ &&
        ${csudo}rm /Applications/TDengine.app/mac_logo.rsrc
    fi
  fi
}

function checkDirectory() {
    if [ ! -d "${bin_link_dir}" ]; then
      ${csudo}mkdir -p ${bin_link_dir}
      log_print "${bin_link_dir} directory created"
    fi

    if [ ! -d "${lib_link_dir}" ]; then
      ${csudo}mkdir -p ${lib_link_dir}
      log_print "${lib_link_dir} directory created"
    fi

    #install log and data dir , then ln to /usr/local/taos
    ${csudo}mkdir -p ${log_dir} && ${csudo}chmod 777 ${log_dir}
    ${csudo}mkdir -p ${data_dir} && ${csudo}chmod 777 ${data_dir}

    ${csudo}rm -rf ${log_link_dir}   || :
    ${csudo}rm -rf ${data_link_dir}  || :

    ${csudo}ln -s ${log_dir} ${log_link_dir}     || :
    ${csudo}ln -s ${data_dir} ${data_link_dir}   || :
}

function install_TDengine() {
    echo -e "${GREEN}Start to install TDengine...${NC}"
    log_print "start to install TDengine"

    checkDirectory

    # Install include, lib, binary and service
    install_include &&
    install_lib &&
    install_jemalloc
    install_bin

    if [[ "$?" != 0 ]];then
      log_print "install TDengine failed!"
      return 1
    fi

    install_config
    install_taosadapter_config
    install_taoskeeper_config
    install_taosadapter_service
    install_taoskeeper_service
    install_service
    install_app

    # Ask if to start the service
    #echo
    #echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    echo
    echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
    if ((${service_mod}==0)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo}systemctl start taosd${NC}"
    elif ((${service_mod}==1)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo}update-rc.d taosd default  ${RED} for the first time${NC}"
        echo -e "                      : ${csudo}service taosd start ${RED} after${NC}"
    else
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ./taosd${NC}"
    fi



    if [ ! -z "$firstEp" ]; then
      tmpFqdn=${firstEp%%:*}
      substr=":"
      if [[ $firstEp =~ $substr ]];then
        tmpPort=${firstEp#*:}
      else
        tmpPort=""
      fi
      if [[ "$tmpPort" != "" ]];then
           echo -e "${GREEN_DARK}To access TDengine    ${NC}: taos -h $tmpFqdn -P $tmpPort${GREEN_DARK} to login into cluster, then${NC}"
      else
          echo -e "${GREEN_DARK}To access TDengine    ${NC}: taos -h $tmpFqdn${GREEN_DARK} to login into cluster, then${NC}"
      fi
      echo -e "${GREEN_DARK}execute ${NC}: create dnode 'newDnodeFQDN:port'; ${GREEN_DARK}to add this new node${NC}"
      echo
    elif [ ! -z "$serverFqdn" ]; then
      echo -e "${GREEN_DARK}To access TDengine    ${NC}: taos -h $serverFqdn${GREEN_DARK} to login into TDengine server${NC}"
      echo
    fi
    log_print "install TDengine successfully!"
    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
}


## ==============================Main program starts from here============================
${csudo}rm -rf ${install_log_path}
log_print "installer start..."
serverFqdn=$(hostname)
install_TDengine
