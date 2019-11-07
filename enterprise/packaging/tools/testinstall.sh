#/bin/bash
#
# This file is used to install TAOS time-series database on
# a linux system. The system is required to use systemd to
# start the service when booting.

set -e

# Variables definition.
datadir="/var/lib/taos"
logdir="/var/log/taos"
configdir="/etc/taos"
java_app_dir="connector"
service_config_dir="/etc/systemd/system"
executable_dir="/usr/local/bin/taos"

# Create tsdbVnode.service.
install_vnode() {
    echo "Installing TSDB Vnode service..."
    # Generate configuration file.
    vnode_serve_config="tsdbVnode.service"
    rm -f ${vnode_serve_config}
    echo "[Unit]"                                                     >> ${vnode_serve_config}
    echo "Description=TSDB Vnode Service"                             >> ${vnode_serve_config}
    echo                                                              >> ${vnode_serve_config}
    echo "[Service]"                                                  >> ${vnode_serve_config}
    echo "Type=simple"                                                >> ${vnode_serve_config}
    echo "ExecStart=/usr/bin/tsdb"                                    >> ${vnode_serve_config}
    echo "StandardOutput=null"                                        >> ${vnode_serve_config}
    echo "Restart=always"                                             >> ${vnode_serve_config}
    # echo 'ExecStop=/bin/kill $MAINPID'                                >> ${vnode_serve_config}
    echo                                                              >> ${vnode_serve_config}
    echo "[Install]"                                                  >> ${vnode_serve_config}
    echo "WantedBy=multi-user.target"                                 >> ${vnode_serve_config}
    # Move configuration file to right place
    sudo mv ${vnode_serve_config} ${service_config_dir}
    sudo chown root:root ${service_config_dir}/${vnode_serve_config}
    # Enable start up
    sudo systemctl enable tsdbVnode
    # Run the service
    sudo systemctl start tsdbVnode
}

# Install management
install_mgmt() {
    echo "Installing TSDB Management service..."
    # Generate configuration file.
    mgmt_serve_config="tsdbMgmt.service"
    rm -f ${mgmt_serve_config}
    echo "[Unit]"                                                       >> ${mgmt_serve_config}
    echo "Description=TSDB Management Service"                          >> ${mgmt_serve_config}
    echo                                                                >> ${mgmt_serve_config}
    echo "[Service]"                                                    >> ${mgmt_serve_config}
    echo "Type=simple"                                                  >> ${mgmt_serve_config}
    echo "ExecStart=/usr/bin/tsmgmt"                                    >> ${mgmt_serve_config}
    echo "StandardOutput=null"                                          >> ${mgmt_serve_config}
    echo "Restart=always"                                               >> ${mgmt_serve_config}
    # echo 'ExecStop=/bin/kill $MAINPID'                                  >> ${mgmt_serve_config}
    echo                                                                >> ${mgmt_serve_config}
    echo "[Install]"                                                    >> ${mgmt_serve_config}
    echo "WantedBy=multi-user.target"                                   >> ${mgmt_serve_config}
    # Move configuration file to right place
    sudo mv ${mgmt_serve_config} ${service_config_dir}
    sudo chown root:root ${service_config_dir}/${mgmt_serve_config}
    # Enable start up
    sudo systemctl enable tsdbMgmt
    # Run the service
    sudo systemctl start tsdbMgmt
}

print_install_guide() {
    echo
    echo "Type 'bash install.sh' to install management and vnode service"
    echo "Type 'bash install.sh vnode' to install vnode service only"
    echo "Type 'bash install.sh mgmt' to install management service only"
}

## Main program starts from here --------------------------

# Make directories needed
if [ -d ${datadir} ]; then
    sudo rm -rf ${datadir}
fi
# /var/lib/taos
sudo mkdir ${datadir}

if [ -d ${logdir} ]; then
    sudo rm -rf ${logdir}
fi
# /var/log/taos
sudo mkdir -m 777 ${logdir}

if [ -d ${configdir} ]; then
    sudo rm -rf ${configdir}
fi
# /etc/taos
sudo mkdir ${configdir}

# Set configuration files.
sudo cp ./cfg/* ${configdir}

# Build the executable files.
make
sudo cp -r ../build/bin ${executable_dir}
sudo mkdir ${executable_dir}/connector

# Establish link to executable files in /usr/bin
link_dir="/usr/bin"
sudo ln -s ${executable_dir}/tsdb ${link_dir}/tsdb
sudo ln -s ${executable_dir}/tsmgmt ${link_dir}/tsmgmt
sudo ln -s ${executable_dir}/taos ${link_dir}/taos

if [ $# == 0 ]; then
    install_mgmt
    install_vnode
elif [ $# == 1 ]; then
    if [ $1 == "tsdb" ]; then
        install_vnode
    elif [ $1 == "mgmt" ]; then
        install_mgmt
    elif [ $1 == "all" ]; then
        install_mgmt
        install_vnode
    elif [ $1 == "-h" ]; then
        print_install_guide
    else
        echo "Wrong argument type."
        print_install_guide
    fi
else
    echo "Wrong argument number."
    print_install_guide
fi

