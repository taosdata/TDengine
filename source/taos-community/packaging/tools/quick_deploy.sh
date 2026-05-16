#!/bin/bash
log_date_time=`date +%m%d%Y-%H%M`
log=${log_date_time}.log
dataDir=/data
logDir=/data/taos/log
 
 
echo "#########collect sysinfo############"
cat /etc/os-release 
free -g
cat /proc/cpuinfo |grep processor
uname -a
df -h 
lsblk 
blkid 
cat /etc/fstab  
mount 
cat /etc/sysctl.conf 
cat /etc/security/limits.conf 
sysctl -p 

 
######check hostname #####
echo "#########hostname"
hostname
hostname -f
echo "#########/etc/hostname"
cat /etc/hostname
echo "#########/etc/hosts"
cat /etc/hosts


echo "#########Step 1############"
echo "$#"
######single node without config file#####
if [ "$#" -eq 0 ] ;then
    echo "#########This is for single node !!"
    echo "#########Please provide config.ini file for cluster installation !!"
    first_ep=`hostname -f`:6030
    second_ep=""
fi


echo "#########Step 2############"
######use config file to configre cluster!#####
echo $1
config_file=$1
if [ "$#" -eq 1 ] ;then 
    echo "#########Use $1 file to configure cluster."
    echo "#########The configure of cluster"
    cat $config_file

first_ep=`sed -n '1p' $config_file |awk '{print $2}'`:6030
second_ep=`sed -n '2p' $config_file |awk '{print $2}'`:6030
fi

echo "#########Step 3############"

################################################################
echo "#########create data directory"
mkdir -p ${dataDir}/taos/{data,core,tmp,soft}
mkdir -p ${logDir}
chmod 777 ${dataDir}/taos/tmp
chmod 777 ${logDir}

echo "#########install suggest package and disable firewall"
os=$(cat /etc/os-release| grep PRETTY_NAME | awk '{print $1}'|awk -F '=' '{print $2}' | sed 's/"//g')
if [ $os = 'Ubuntu' ]
then
    echo "This is Ununtu!"
	#apt install screen tmux gdb fio iperf3 sysstat net-tools ntp tree wget 
	ufw status
	ufw stop
	ufw disable
elif [ $os = 'CentOS' -o $os = 'Red' ] 
then
    echo "This is Centos/Red"
	#yum install -y screen tmux gdb fio iperf3 sysstat net-tools ntp tree wget
	systemctl status firewalld 
	systemctl stop firewalld 
	systemctl disable firewalld 
else
    echo "####$os"
fi

sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
 

echo "#########system tunning"
echo "fs.nr_open = 10485760" >>/etc/sysctl.conf 
echo "net.core.somaxconn=10240" >> /etc/sysctl.conf
echo "net.core.netdev_max_backlog=20480" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog=10240" >> /etc/sysctl.conf
echo "net.ipv4.tcp_retries2=5" >> /etc/sysctl.conf
echo "net.ipv4.tcp_syn_retries=2" >> /etc/sysctl.conf
echo "net.ipv4.tcp_synack_retries=2" >> /etc/sysctl.conf
echo "net.ipv4.tcp_tw_reuse=1" >> /etc/sysctl.conf
echo "net.ipv4.tcp_tw_recycle=1" >> /etc/sysctl.conf
echo "net.ipv4.tcp_keepalive_time=600" >> /etc/sysctl.conf
echo "net.ipv4.tcp_abort_on_overflow=1" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_tw_buckets=5000" >> /etc/sysctl.conf
echo "net.ipv4.ip_local_port_range=10000 60999" >> /etc/sysctl.conf

echo "* soft ntaosc  65536" >>/etc/security/limits.conf
echo "* soft nofile 1048576" >>/etc/security/limits.conf
echo "* soft stack  65536" >>/etc/security/limits.conf
echo "* hard ntaosc  65536" >>/etc/security/limits.conf
echo "* hard nofile 1048576" >>/etc/security/limits.conf
echo "* hard stack  65536" >>/etc/security/limits.conf
echo "root soft ntaosc  65536" >>/etc/security/limits.conf
echo "root soft nofile 1048576" >>/etc/security/limits.conf
echo "root soft stack  65536" >>/etc/security/limits.conf
echo "root hard ntaosc  65536" >>/etc/security/limits.conf
echo "root hard nofile 1048576" >>/etc/security/limits.conf
echo "root hard stack  65536" >>/etc/security/limits.conf

echo "ulimit -c unlimited" >>/etc/profile 
echo "kernel.core_pattern=${dataDir}/taos/core/core-%e-%p" >>/etc/sysctl.conf 
sysctl -p
date

####to do check date timezone and ntp########
#cp -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
#NTP
#SWAP
#reboot


echo "#########install TDengine"

./install.sh -e no
pwd 

echo "#########Create /etc/taos/taos.cfg"
mv /etc/taos/taos.cfg /etc/taos/taos.cfg.bak

cat /proc/cpuinfo | grep "processor" | wc -l
num=`cat /proc/cpuinfo | grep "processor" | wc -l`
cpunum=$(expr $num + $num) 
host_name=`hostname -f`

#####vi /etc/taos/taos.cfg
echo "firstEp                     $first_ep
secondEp                    $second_ep
fqdn                        $host_name
supportVnodes               $cpunum
logDir                      $logDir
dataDir                     ${dataDir}/taos/data
tempDir                     ${dataDir}/taos/tmp
keepColumnName              0
maxNumOfDistinctRes         10000000
timezone                    UTC-8
locale                      en_US.UTF-8
charset                     UTF-8
maxShellConns               100000
maxConnections              100000
audit                       0
auditFqdn                   localhost
monitor                     1 
monitorFqdn                 localhost
logKeepDays                 10
debugflag                   131
shellActivityTimer          120
numOfRpcSessions            30000" >> /etc/taos/taos.cfg
########################
########################tunning taosadapter and taosx log directory
sed -i "/^\#*path/c\path\ =\ \"${logDir}\"" /etc/taos/taosadapter.toml
sed -i "/^\#*logs_home/c\logs_home\ =\ \"${logDir}\"" /etc/taos/taosx.toml
##########################single nodes
echo "#########Start toasd"
./start-all.sh
date
if [ "$#" -eq 0 ] ;then
sleep 5
taos -s "show dnodes;"
taos -s "show cluster\G;"
echo "########"
taosd -k
curl -u root:taosdata ${host_name}:6041/rest/sql -d "select server_version()"
echo -e "\r"
echo "#########Installation completed########"
echo "#########Suggest reboot OS##########"
date
exit
fi
##########################use $config_file to config cluster
if test "$host_name"x = `sed -n '1p' $config_file |awk '{print $2}'`x ;then 
echo "#########Add dnodes and mnodes"
while read ip_address host_name 
do
echo "$host_name"
taos -s "create dnode '$host_name:6030';"
sleep 5
done <$config_file
fi


host_name=`hostname -f`
if test "$host_name"x = `sed -n '3p' $config_file |awk '{print $2}'`x ;then 
echo "#########Add mnodes"
taos -s "create mnode on dnode 2;" 
sleep 5
taos -s "create mnode on dnode 3;"
fi

sleep 5
taos -s "show dnodes;"
taos -s "show mnodes;"
taos -s "show grants\G;"
taos -s "show cluster\G;"
echo "########"
taosd -k
echo "#########Test TDengine"
curl -u root:taosdata ${host_name}:6041/rest/sql -d "select server_version()"
echo -e "\r"
echo "#########Installation completed########"
echo "#########Suggest reboot OS##########"
date
cd ..
