#!/bin/bash
#
# This file is used to install tdengine rpm package on centos systems. The operating system 
# is required to use systemd to manage services at boot
#set -x

iplist=""
serverFqdn=""

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
# Dynamic directory
data_dir="/var/lib/taos"
log_dir="/var/log/taos"
data_link_dir="/usr/local/taos/data"
log_link_dir="/usr/local/taos/log"
install_main_dir="/usr/local/taos"

# static directory
cfg_dir="/usr/local/taos/cfg"
bin_dir="/usr/local/taos/bin"
lib_dir="/usr/local/taos/driver"
init_d_dir="/usr/local/taos/init.d"
inc_dir="/usr/local/taos/include"

cfg_install_dir="/etc/taos"
bin_link_dir="/usr/bin"
lib_link_dir="/usr/lib"
lib64_link_dir="/usr/lib64"
inc_link_dir="/usr/include"

service_config_dir="/etc/systemd/system"


# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo"
fi

initd_mod=0
service_mod=2
if pidof systemd &> /dev/null; then
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

function kill_taosd() {
#  ${csudo} pkill -f taosd || :
  pid=$(ps -ef | grep "taosd" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function install_include() {
    ${csudo} rm -f ${inc_link_dir}/taos.h ${inc_link_dir}/taoserror.h|| :
    ${csudo} ln -s ${inc_dir}/taos.h ${inc_link_dir}/taos.h  
    ${csudo} ln -s ${inc_dir}/taoserror.h ${inc_link_dir}/taoserror.h  
}

function install_lib() {
    ${csudo} rm -f ${lib_link_dir}/libtaos* || :
    ${csudo} rm -f ${lib64_link_dir}/libtaos* || :
    
    ${csudo} ln -s ${lib_dir}/libtaos.* ${lib_link_dir}/libtaos.so.1
    ${csudo} ln -s ${lib_link_dir}/libtaos.so.1 ${lib_link_dir}/libtaos.so
    
    if [[ -d ${lib64_link_dir} && ! -e ${lib64_link_dir}/libtaos.so ]]; then
      ${csudo} ln -s ${lib_dir}/libtaos.* ${lib64_link_dir}/libtaos.so.1           || :
      ${csudo} ln -s ${lib64_link_dir}/libtaos.so.1 ${lib64_link_dir}/libtaos.so   || :
    fi
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taos     || :
    ${csudo} rm -f ${bin_link_dir}/taosd    || :
    ${csudo} rm -f ${bin_link_dir}/taosdemo || :
    ${csudo} rm -f ${bin_link_dir}/taosdemox || :
    ${csudo} rm -f ${bin_link_dir}/taosdump || :
    ${csudo} rm -f ${bin_link_dir}/rmtaos   || :
    ${csudo} rm -f ${bin_link_dir}/set_core || :

    ${csudo} chmod 0555 ${bin_dir}/*

    #Make link
    [ -x ${bin_dir}/taos ] && ${csudo} ln -s ${bin_dir}/taos ${bin_link_dir}/taos             || :
    [ -x ${bin_dir}/taosd ] && ${csudo} ln -s ${bin_dir}/taosd ${bin_link_dir}/taosd          || :
    [ -x ${bin_dir}/taosdemo ] && ${csudo} ln -s ${bin_dir}/taosdemo ${bin_link_dir}/taosdemo || :
    [ -x ${bin_dir}/taosdemox ] && ${csudo} ln -s ${bin_dir}/taosdemox ${bin_link_dir}/taosdemox || :
    [ -x ${bin_dir}/taosdump ] && ${csudo} ln -s ${bin_dir}/taosdump ${bin_link_dir}/taosdump || :
    [ -x ${bin_dir}/set_core.sh ] && ${csudo} ln -s ${bin_dir}/set_core.sh ${bin_link_dir}/set_core || :
}

function add_newHostname_to_hosts() {
  localIp="127.0.0.1"
  OLD_IFS="$IFS"
  IFS=" "
  iphost=$(cat /etc/hosts | grep $1 | awk '{print $1}')
  arr=($iphost)
  IFS="$OLD_IFS"
  for s in ${arr[@]}
  do
    if [[ "$s" == "$localIp" ]]; then
      return
    fi
  done 
  ${csudo} echo "127.0.0.1  $1" >> /etc/hosts   ||:
}

function set_hostname() {
  echo -e -n "${GREEN}Please enter one hostname(must not be 'localhost')${NC}:"
	read newHostname
  while true; do
    if [[ ! -z "$newHostname" && "$newHostname" != "localhost" ]]; then
      break
    else
      read -p "Please enter one hostname(must not be 'localhost'):" newHostname
    fi
  done

  ${csudo} hostname $newHostname ||:
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
    ${csudo} echo $newHostname > /etc/hostname   ||:
  fi
  
  #debian: #HOSTNAME=yourname
  if [[ -e /etc/sysconfig/network ]]; then
    ${csudo} sed -i -r "s/#*\s*(HOSTNAME=\s*).*/\1$newHostname/" /etc/sysconfig/network   ||:
  fi

  ${csudo} sed -i -r "s/#*\s*(fqdn\s*).*/\1$newHostname/" ${cfg_install_dir}/taos.cfg
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
  for s in ${arr[@]}
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
    ${csudo} sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/taos.cfg    
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
          ${csudo} sed -i -r "s/#*\s*(fqdn\s*).*/\1$localFqdn/" ${cfg_install_dir}/taos.cfg    
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
			    echo "Invalid input..."
			    ;;
	      esac
	    fi
    done
  fi
}

function install_config() {
    if [ ! -f ${cfg_install_dir}/taos.cfg ]; then
        ${csudo} ${csudo} mkdir -p ${cfg_install_dir}
        [ -f ${cfg_dir}/taos.cfg ] && ${csudo} cp ${cfg_dir}/taos.cfg ${cfg_install_dir}
        ${csudo} chmod 644 ${cfg_install_dir}/*
    fi
    
    # Save standard input to 6 and open / dev / TTY on standard input
    exec 6<&0 0</dev/tty 

    local_fqdn_check
    
    # restore the backup standard input, and turn off 6
    exec 0<&6 6<&-

    ${csudo} mv ${cfg_dir}/taos.cfg ${cfg_dir}/taos.cfg.org
    ${csudo} ln -s ${cfg_install_dir}/taos.cfg ${cfg_dir}
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
            ${csudo} sed -i -r "s/#*\s*(firstEp\s*).*/\1$firstEp/" ${cfg_install_dir}/taos.cfg    
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
                ${csudo} bash -c "echo $emailAddr > ${email_file}"
                break         
            #else
            #    read -p "Please enter the correct email address: " emailAddr   
            #fi
        else
            break
        fi
    done	
}

function clean_service_on_sysvinit() {
    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo} sed -i "\|${restart_config_str}|d" /etc/inittab || :    
    
    if pidof taosd &> /dev/null; then
        ${csudo} service taosd stop || :
    fi
    
    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --del taosd || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv -r taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d -f taosd remove || :
    fi
    
    ${csudo} rm -f ${service_config_dir}/taosd || :
    
    if $(which init &> /dev/null); then
        ${csudo} init q || :
    fi
}

function install_service_on_sysvinit() {
    clean_service_on_sysvinit

    sleep 1

    # Install taosd service  
    ${csudo} cp %{init_d_dir}/taosd ${service_config_dir} && ${csudo} chmod a+x ${service_config_dir}/taosd

    #restart_config_str="taos:2345:respawn:${service_config_dir}/taosd start"
    #${csudo} grep -q -F "$restart_config_str" /etc/inittab || ${csudo} bash -c "echo '${restart_config_str}' >> /etc/inittab"
    
    if ((${initd_mod}==1)); then
        ${csudo} chkconfig --add taosd || :
        ${csudo} chkconfig --level 2345 taosd on || :
    elif ((${initd_mod}==2)); then
        ${csudo} insserv taosd || :
        ${csudo} insserv -d taosd || :
    elif ((${initd_mod}==3)); then
        ${csudo} update-rc.d taosd defaults || :
    fi
}

function clean_service_on_systemd() {
    taosd_service_config="${service_config_dir}/taosd.service"

    # taosd service already is stoped before install in preinst script
    #if systemctl is-active --quiet taosd; then
    #    echo "TDengine is running, stopping it..."
    #    ${csudo} systemctl stop taosd &> /dev/null || echo &> /dev/null
    #fi
    ${csudo} systemctl disable taosd &> /dev/null || echo &> /dev/null

    ${csudo} rm -f ${taosd_service_config}
}

# taos:2345:respawn:/etc/init.d/taosd start

function install_service_on_systemd() {
    clean_service_on_systemd

    taosd_service_config="${service_config_dir}/taosd.service"

    ${csudo} bash -c "echo '[Unit]'                             >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Description=TDengine server service' >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'After=network-online.target'        >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Wants=network-online.target'        >> ${taosd_service_config}"
    ${csudo} bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo} bash -c "echo '[Service]'                          >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Type=simple'                        >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'ExecStart=/usr/bin/taosd'           >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'ExecStartPre=/usr/local/taos/bin/startPre.sh'           >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitNOFILE=infinity'               >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitNPROC=infinity'                >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'LimitCORE=infinity'                 >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'TimeoutStartSec=0'                  >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StandardOutput=null'                >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'Restart=always'                     >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StartLimitBurst=3'                  >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'StartLimitInterval=60s'             >> ${taosd_service_config}"
    ${csudo} bash -c "echo                                      >> ${taosd_service_config}"
    ${csudo} bash -c "echo '[Install]'                          >> ${taosd_service_config}"
    ${csudo} bash -c "echo 'WantedBy=multi-user.target'         >> ${taosd_service_config}"
    ${csudo} systemctl enable taosd
}

function install_service() {
    if ((${service_mod}==0)); then
        install_service_on_systemd
    elif ((${service_mod}==1)); then
        install_service_on_sysvinit
    else
        # manual start taosd
        kill_taosd
    fi
}

function install_TDengine() {
    echo -e "${GREEN}Start to install TDengine...${NC}"

    #install log and data dir , then ln to /usr/local/taos
    ${csudo} mkdir -p ${log_dir} && ${csudo} chmod 777 ${log_dir}
    ${csudo} mkdir -p ${data_dir} 
    
    ${csudo} rm -rf ${log_link_dir}   || :
    ${csudo} rm -rf ${data_link_dir}  || :
    
    ${csudo} ln -s ${log_dir} ${log_link_dir}     || :
    ${csudo} ln -s ${data_dir} ${data_link_dir}   || :
    
    # Install include, lib, binary and service
    install_include
    install_lib
    install_bin
    install_service
    install_config	

    # Ask if to start the service
    #echo
    #echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
    echo
    echo -e "${GREEN_DARK}To configure TDengine ${NC}: edit /etc/taos/taos.cfg"
    if ((${service_mod}==0)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} systemctl start taosd${NC}"
    elif ((${service_mod}==1)); then
        echo -e "${GREEN_DARK}To start TDengine     ${NC}: ${csudo} update-rc.d taosd default  ${RED} for the first time${NC}"
        echo -e "                      : ${csudo} service taosd start ${RED} after${NC}"
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
    echo
    echo -e "\033[44;32;1mTDengine is installed successfully!${NC}"
}


## ==============================Main program starts from here============================
serverFqdn=$(hostname)
install_TDengine
