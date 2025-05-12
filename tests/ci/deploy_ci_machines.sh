#!/bin/bash

function scp_file_from_host {
    # check at least three parameters
    if [ "$#" -lt 3 ]; then
        echo "Usage: $0 host passwd source_filename [dest_filename]"
        exit 1
    fi

    host=$1
    passwd=$2
    source_filename=$3
    # If the fourth parameter is not provided, use the third parameter as the default value
    dest_filename=${4:-$3}  

    # use sshpass and scp for secure file transfer
    sshpass -p "$passwd" scp  -o StrictHostKeyChecking=no -r  "$host":"$source_filename" "$dest_filename"
}


# install docker and sshpass
curl -fsSL https://mirrors.ustc.edu.cn/docker-ce/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://mirrors.ustc.edu.cn/docker-ce/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install  -y docker-ce sshpass jq 
sudo systemctl enable docker
sudo systemctl start docker

# create a log directory
mkdir -p /var/lib/jenkins/workspace/log

# Assuming you have a file called 'file_list.txt' with one filename per line
file_list="ci_deploy_dependency_file_list.txt"
monitorip="192.168.1.59"
passwd_all="abcdefg"

# Read the file list and call scp_file_from_host for each file
while IFS= read -r source_filename; do
    scp_file_from_host "$monitorip" "$passwd_all" "$source_filename"
done < "$file_list"

# modify the configuration file
ip=$(ifconfig |grep inet|grep 192  |awk  '{print $2}')
sed -i "s/${monitorip}/$ip/" /home/log_server.json
sed -i "s/${monitorip}/$ip/" /home/m.json

#mkdir corefile dir and configure the system to automatically set corefile dir at startup
mkdir -p /home/coredump/ &&  echo "echo '/home/coredump/core_%e-%p' | sudo tee /proc/sys/kernel/core_pattern " >> /root/.bashrc


# get  image from 0.212
image_ip="192.168.0.212"
scp_file_from_host $image_ip $passwd_all "/home/tang/work/image/taos_image.tar " "/home/taos_image.tar"
docker load -i /home/taos_image.tar

#start http server 
nohup /var/lib/jenkins/workspace/log/start_http.sh  &

# start CI monitor and remove corefile in crontable
(crontab -l;echo "0 1 * * * /usr/bin/bash /var/lib/jenkins/workspace/remove_corefile.sh") | crontab
(crontab -l;echo "@reboot /usr/bin/bash  /var/lib/jenkins/workspace/start_CI_Monitor.sh") | crontab


# generate cache dir
cd  /var/lib/jenkins/workspace/TDinternal/community/tests/parallel_test || exit
time ./container_build_newmachine.sh -w /var/lib/jenkins/workspace -e

# test if the CI machine compilation is successful
time ./container_build.sh -w /var/lib/jenkins/workspace -e