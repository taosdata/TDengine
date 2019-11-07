#/bin/sh
#
# Script to stop the service and uninstall TSDB


service_config_dir="/etc/systemd/system"
link_dir="/usr/bin"
executable_dir="/usr/local/bin/taos"
datadir="/var/lib/taos"
logdir="/var/log/taos"
configdir="/etc/taos"

# Stop service and disable booting start.
sudo systemctl stop tsdbVnode
sudo systemctl stop tsdbMgmt
sudo systemctl disable tsdbVnode
sudo systemctl disable tsdbMgmt

# Remove service configuration files.
sudo rm -f ${service_config_dir}/tsdbVnode.service
sudo rm -f ${service_config_dir}/tsdbMgmt.service

# Remove links
sudo rm -f ${link_dir}/tsmgmt
sudo rm -f ${link_dir}/tsdb
sudo rm -f ${link_dir}/taos

# Remove the executable files
sudo rm -rf ${executable_dir}

# Remove all created directories.
sudo rm -rf ${datadir} 
sudo rm -rf ${logdir} 
sudo rm -rf ${configdir} 
