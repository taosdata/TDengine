#!/bin/bash
set -e
# set -x

script_home=$(dirname $(readlink -m $0))
nCores=$(grep -c ^processor /proc/cpuinfo) # number of cores
dnode="${script_home}/dnode.sh"
root_dir=$(readlink -m ${script_home}/../../..)
dnode_home=${root_dir}/dnodes
result_file=${root_dir}/test_result
nservers=3
query_interval=3
kill_interval=10

function assign_core() {
    sudo taskset -acp $2 $1 > /dev/null
}

function my_exit() {
    killall testPerf.sh
    killall testTBase
    $dnode clean all

    exit 1
}


while true; do
    # check the status
    if $dnode status | grep -q off;then
        my_exit
    fi

    idnode=$(( ( RANDOM % $nservers )  + 1 ))

    echo $(date +"%F %T"): Starting to stop dnode$idnode
    $dnode stop $idnode || my_exit

    sleep 1

    # Check the status
    for dnode_dir in ${dnode_home}/dnode*; do
        dnode_name=$(basename $dnode_dir)


        if [ ${dnode_name} = dnode${idnode} ];then
            if [ $($dnode status | grep $dnode_name | awk '{ print $4 }') = "on" ];then
                echo Checking $dnode_name
                my_exit
            fi
        else
            if [ $($dnode status | grep $dnode_name | awk '{ print $4 }') = "off" ];then
                echo Checking $dnode_name
                my_exit
            fi
        fi
    done

    $dnode status


    # check results
    start_time=$(date +%s)
    while true; do
        if grep -q Failed ${result_file}; then
            echo "query failure"
            my_exit
        fi
        end_time=$(date +%s)
        if (( $((end_time-start_time)) > $kill_interval )); then
            break
        fi
        sleep $query_interval
    done

    sleep $kill_interval

    i=""
    for ((i=1;i<=5;i++)); do
        echo $(date +"%F %T"): Trying to start dnode$idnode time $i
        $dnode start $idnode && break
    done
    if [ $i -gt 5 ]; then
        my_exit
    fi

    # check the status
    if $dnode status | grep -q off;then
        my_exit
    fi

    # Assign core
    # pid_file="${dnode_home}/dnode${idnode}/dnodePid"
    # pid=$(cat $pid_file)
    # assign_core $pid $(($nCores-$idnode))

    $dnode status

    sleep $kill_interval
done
