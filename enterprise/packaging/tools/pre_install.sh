#!/bin/bash

##################################################
# 
# Preparation before installing
#
##################################################

set -e

echo "------------------------------------------------------------------------"
echo "Start preparing envrionment before installation..."
echo "------------------------------------------------------------------------"

if [ -d "/tmp/taos/" ]; then
   rm -rf /tmp/taos
fi

# Get responsible directories
if [ "${tools_dir}" == "" ]; then
   tools_dir=$(pwd);
fi
code_dir="$(readlink -m ${tools_dir}/..)"
top_dir="$(readlink -m ${code_dir}/..)"
release_dir="${top_dir}/release"
taos_tmp_dir="/tmp/taos/"

if [ ! -d "${taos_tmp_dir}" ]; then 
   mkdir ${taos_tmp_dir} 
fi 

cp ${release_dir}/taos*-${USER}*.tar.gz ${taos_tmp_dir} 

cd ${taos_tmp_dir}

tar -xzvf taos*-${USER}*.tar.gz

cd taos*-${USER}*
#source ./install.sh

#cd ${tools_dir}
echo "------------------------------------------------------------------------"
echo "Preparing envrionment before installation Done!"
echo "------------------------------------------------------------------------"









