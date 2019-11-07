#!/bin/bash
#
# keepalived control files for systemd
#
service_config_dir="/etc/systemd/system"

node=$1

install_keepalived() {
    keepalived_serve_config="keepalived.service"

    echo "Installing keepalived service..."
    if (( $(ps -ef | grep -v grep | grep keepalived | wc -l) > 0 )); then
        sudo systemctl stop keepalived.service
        sudo systemctl disable keepalived.service
    fi

    rm -f ${keepalived_serve_config}
    echo "[Unit]"                                                      >> ${keepalived_serve_config}
    echo "Description=LVS and VRRP High Availability monitor"          >> ${keepalived_serve_config}
    echo "After=network.target"                                        >> ${keepalived_serve_config}
    echo "ConditionFileNotEmpty=/etc/keepalived/keepalived.conf"       >> ${keepalived_serve_config}
    echo                                                               >> ${keepalived_serve_config}
    echo "[Service]"                                                   >> ${keepalived_serve_config}
    echo "Type=simple"                                                 >> ${keepalived_serve_config}
    echo "EnvironmentFile=-/etc/keepalived/keepalived.conf"            >> ${keepalived_serve_config}

    if [ $node == "master" ]; then
        echo "ExecStart=/sbin/keepalived --dont-fork -f /etc/keepalived/keepalived-master.conf" >> ${keepalived_serve_config}
    else 
        echo "ExecStart=/sbin/keepalived --dont-fork -f /etc/keepalived/keepalived-backup.conf" >> ${keepalived_serve_config}
    fi

    echo "ExecReload=/bin/kill -s HUP $MAINPID"                        >> ${keepalived_serve_config}
    echo "KillMode=process"                                            >> ${keepalived_serve_config}
    echo                                                               >> ${keepalived_serve_config}
    echo "[Install]"                                                   >> ${keepalived_serve_config}
    echo "WantedBy=multi-user.target"                                  >> ${keepalived_serve_config}
    # Move configuration file to right place
    sudo mv ${keepalived_serve_config} ${service_config_dir}
    sudo chown root:root ${service_config_dir}/${keepalived_serve_config}
    # Enable start up
    sudo systemctl preset keepalived.service
    sudo systemctl enable keepalived.service
    sudo systemctl start keepalived.service
}


