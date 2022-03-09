#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -o old version package"
    echo -e "\t -n new version package"
    echo -e "\t -c client package"
    echo -e "\t -h help"
}

while getopts "w:o:n:c:h" opt; do
    case $opt in
        w)
            WORK_DIR=$OPTARG
            ;;
        o)
            TAOS_PKG1=$OPTARG
            ;;
        n)
            TAOS_PKG2=$OPTARG
            ;;
        c)
            CLIENT_PKG=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            exit 0
            ;;
    esac
done

if [ -z "$WORK_DIR" ]; then
    usage
    exit 1
fi
if [ -z "$TAOS_PKG1" ]; then
    usage
    exit 1
fi
if [ -z "$TAOS_PKG2" ]; then
    usage
    exit 1
fi
if [ ! -z "$CLIENT_PKG" ]; then
    if [ ! -f "$CLIENT_PKG" ]; then
        echo "$CLIENT_PKG not found"
        exit 1
    fi
fi

script_dir=`dirname $0`
cd $script_dir

source settings.sh

code_dir=$WORK_DIR/TDinternal
container_workdir1=$WORK_DIR/compatibility/$container_name1
container_workdir2=$WORK_DIR/compatibility/$container_name2
container_workdir3=$WORK_DIR/compatibility/$container_name3
container_workdir4=$WORK_DIR/compatibility/$container_name4


mkdir -p $container_workdir1
mkdir -p $container_workdir2
mkdir -p $container_workdir3
mkdir -p $container_workdir4

docker rm -f $container_name1 >/dev/null 2>&1
docker rm -f $container_name2 >/dev/null 2>&1
docker rm -f $container_name3 >/dev/null 2>&1
docker rm -f $container_name4 >/dev/null 2>&1

net_name=mynet
docker network create --driver bridge --subnet 172.31.30.0/24 --gateway 172.31.30.1 $net_name

./init_container.sh -d $code_dir -w $container_workdir1 -p $TAOS_PKG1 -q $TAOS_PKG2 -n $net_name -c $container_name1 &
./init_container.sh -d $code_dir -w $container_workdir2 -p $TAOS_PKG1 -q $TAOS_PKG2 -n $net_name -c $container_name2 &
./init_container.sh -d $code_dir -w $container_workdir3 -p $TAOS_PKG1 -q $TAOS_PKG2 -n $net_name -c $container_name3 &
./init_container.sh -d $code_dir -w $container_workdir4 -p $TAOS_PKG1 -q $TAOS_PKG2 -n $net_name -c $container_name4 &

RET=0
pids=`jobs -p`
for pid in $pids; do
    wait $pid
    status=$?
    if [ $status -ne 0 ]; then
        echo "init container $pid status is $status!"
        RET=$status
    fi
done

if [ $RET -eq 0 ]; then
    if [ -z "$CLIENT_PKG" ]; then
        docker exec $container_name4 pip3 install /home/TDinternal/community/src/connector/python
        RET=$?
    else
        pkg_name=`basename $CLIENT_PKG`
        pkg_dir=`echo "$pkg_name"|sed "s/-Linux-x64.tar.gz//"`
        docker cp $CLIENT_PKG $container_name4:/home/
        docker exec $container_name4 sh -c "cd /home;tar xzf $pkg_name;if [ -d /home/$pkg_dir/connector/python/linux/python3 ]; then pip3 install /home/$pkg_dir/connector/python/linux/python3; else pip3 install /home/$pkg_dir/connector/python; fi"
        RET=$?
    fi
fi

if [ $RET -eq 0 ]; then
    echo "containers created"
else
    echo "containers create failed"
fi

exit $RET

