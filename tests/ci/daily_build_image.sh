#!/bin/bash

set -x

script_dir=`dirname $0`
cd $script_dir
script_dir=`pwd`
cd $script_dir/repository/taos-connector-python
git pull

cd $script_dir/repository/TDinternal
git clean -fxd
git pull

cd $script_dir/repository/TDinternal/community
git clean -fxd
git checkout  main 
git pull origin main 
git submodule update --init --recursive

cd $script_dir
cp $script_dir/repository/TDinternal/community/tests/ci/build_image.sh  .
cp $script_dir/repository/TDinternal/community/tests/ci/daily_build_image.sh  .

./build_image.sh || exit 1
docker image prune -f 
ips="\
192.168.1.47 \
192.168.1.48 \
192.168.1.49 \
192.168.1.52 \
192.168.0.215 \
192.168.0.217 \
192.168.0.219 \
"

image=taos_image.tar

docker save taos_test:v1.0 -o $image

for ip in $ips; do
    echo "scp $image root@$ip:/home/ &"
    scp $image root@$ip:/home/ &
done
wait

for ip in $ips; do
    echo "ssh root@$ip docker load -i /home/$image &"
    ssh root@$ip docker load -i /home/$image &
done
wait

for ip in $ips; do
    echo "ssh root@$ip rm -f /home/$image &"
    ssh root@$ip rm -f /home/$image &
done
wait

rm -rf  taos_image.tar

