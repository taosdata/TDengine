#!/bin/bash


#############################################
#
#  Taos TDengine pre-build shell, just for 
#  preparing code and ENV before doing build
#
#############################################


set -e

echo "------------------------------------------------------------------------"
echo "Start preparing envrionment before building project..."
echo "------------------------------------------------------------------------"

# Get responsible directories
if [ "${tools_dir}" == "" ]; then
   tools_dir=$(pwd);
fi
code_dir="$(readlink -m ${tools_dir}/..)"
top_dir="$(readlink -m ${code_dir}/..)"
release_dir="${top_dir}/release"

rm -rf ${release_dir}/*

cd ${code_dir}
#git pull origin master
cd ${tools_dir}
echo "------------------------------------------------------------------------"
echo "Prepare envrionment before building Done!"
echo "------------------------------------------------------------------------"


