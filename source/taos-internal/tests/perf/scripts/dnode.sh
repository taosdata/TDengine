#!/bin/bash
set -e
# set -x
########################################################################################################
# dnode.sh
#     This script is used to simulate a TDengine cluster on a computer. Users can
#     get simple instructions when run this script withou any option. The create dnode
#     files are in ../..
#
# 1 setup
#
#     Users can use the setup option of dnode.sh script to set up a simulated TDengine cluster.
#
#     For example:
#          ./dnode.sh setup
#
#     This command will setup a cluster with 3 nodes by default. Users can also assign the number
#     of nodes in the clusters.
#
#     For example:
#          ./dnode.sh setup 5
#
#     Command above setup a TDengine clusters with 5 nodes.
#
# 2 start
#
#     Users can also start a node using the start option of dnode.sh. This option will create a dnode
#     and start it if the dnode does not exist. If the dnode exists, this option will start the dnode
#     if it is stopped.
#
#     For example:
#          ./dnode.sh start 1
#
# 3 stop
#     Users can stop a dnode with the stop option of dnode.sh. When a dnode is stopped, the directories and
#     files are kept but the process is killed.
#
#     For example:
#          ./dnode.sh stop 1
#
# 4 restart
#     The restart option is used to restart the dnode process.
#
#     For example:
#          ./dnode.sh retart 1
#
# 5 status
#     This option is used to check the dnode status.
#
#     For example:
#          ./dnode.sh status
#
# 6 connect
#     The connect option can be used to launch a TDengine shell connected to the cluster. The default dnode connected
#     to is dnode1. Users can also assign the which number to connect
#
#     For example:
#          ./dnode.sh connect             # Default to connect dnode1
#
#     For example:
#          ./dnode.sh connect  2          # Connect to dnode2
#
# 7 drop
#     The drop option is used to drop a dnode in the cluster. The dnode process is killed and the directories and
#     files about the dnode are gone.
#
#     For example:
#          ./dnode.sh drop 3
#
# 8 clean
#     Use the clean option to drop all the dnodes.
#
#     For example:
#          ./dnode.sh clean
########################################################################################################




RED_COLOR='\033[0;31m'
GREEN_COLOR='\033[0;32m'
BROWN_COLOR='\033[0;33m'
BLUE_COLOR='\033[0;96m'
NO_COLOR='\033[0m'

# Glocal variable definition-----------------------------------------------------
base_home=$(readlink -f "$(dirname $(readlink -f $0))/..")
script_home=${base_home}/scripts
ip_prefix="192.168.0."

root_home=$(readlink -f ${base_home}/../..)
code_home=${root_home}/taosdata
dnode_home=${root_home}/dnodes

taosd="${root_home}/build/bin/taosd"
# if [ ! -x $taosd ]; then
#     taosd="/usr/local/bin/taos/taosd"
# fi

taos="${root_home}/build/bin/taos"
# if [ ! -x $taos ]; then
#     taos="/usr/local/bin/taos/taos"
# fi

# Function definition-----------------------------------------------------------
function print_help() {
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}setup   [ndnodes] [debugFlag]${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}start   idnode    [debugFlag]${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}stop    idnode    [signal number]${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}drop    idnode${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}restart idnode${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}connect idnode${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}status${NO_COLOR}"
    echo -e "${BROWN_COLOR}dnode ${GREEN_COLOR}clean${NO_COLOR}"
}

function is_dnode_running() {
    idnode=$1
    dnode_dir=${dnode_home}/dnode${idnode}
    pid_file=${dnode_dir}/dnodePid
    if [ -f $pid_file ]; then
        pid=$(cat $pid_file)
        if grep -q taosd /proc/$pid/cmdline &> /dev/null; then
            return 0
        fi
    fi

    return 1
}

function stop_dnode() {
    idnode=$1
    test ! -z $2 && signum=$2 || signum=15

    if is_dnode_running $idnode; then
        pid=$(cat ${dnode_home}/dnode${idnode}/dnodePid)
        kill -$signum $pid
    fi

    sleep 0.5

    # Check if it is stop
    if is_dnode_running $idnode; then
        return 1
    fi

    return 0
}

function cleanup() {
    if [ ! -z $1 ]; then
        if systemctl is-active --quiet taosd; then
            echo -e "${RED_COLOR}TDengine is still running! Use \"sudo systemctl stop taosd\" to stop it.${NO_COLOR}"
            exit 0
        fi
    fi

    if pgrep taosd &> /dev/null; then
        for dnode_dir in $dnode_home/dnode*; do
            if [ -d $dnode_dir ]; then
                idnode=$(echo $(basename $dnode_dir) | tr -dc '0-9')
                if_file=${dnode_home}/dnode${idnode}/dnodeIF
                if_name=$(cat $if_file)

                sudo ifconfig $if_name down &> /dev/null || echo &> /dev/null
                stop_dnode $idnode || return 1
            fi
        done
    fi

    if [ -z $1 ];then
        rm -rf ${dnode_home}
    fi

    if [ ! -z $1 ]; then
        if pgrep taosd &> /dev/null; then
            echo -e "${RED_COLOR}Other TDengine daemon is running. Please stop it!${NO_COLOR}"
            exit 1
        fi
    fi
}



function start_dnode() { # start_dnode 1 $is_run <131>
    if [ $# -lt 1 ];then
        echo "start_dnode requires at least one parameter"
        echo "Usage: node 1 [131]"
        return 1
    fi

    idnode=$1

    debug_flag=135
    if [ ! -z $3 ]; then
        debug_flag=$3
    fi
    tar_file=${root_home}/taos.tar.gz
    build_home=${root_home}/build

    # Initialize the dnode file
    if  [ ! -d ${dnode_home} ]; then

        if [ ! -x $taosd ]; then
            if [ ! -e ${tar_file} ]; then
                return 1;
            else
                curr_dir=$(pwd)
                cd ${root_home}
                tar -zxf taos.tar.gz
                mkdir -p ${build_home}/bin | :
                cp bin/taosd bin/taos ${build_home}/bin && chmod +x ${build_home}/bin/*
                rm -rf $(tar -tf taos.tar.gz)
                cd ${curr_dir}
            fi
        fi

        [ ! -x $taosd ] && return 1

        # Establish the environment
        mkdir -p ${dnode_home}
    fi

    dnode_dir=${dnode_home}/dnode${idnode}
    pid_file=${dnode_dir}/dnodePid

    if [ -d $dnode_dir ]; then
        if ! is_dnode_running $idnode; then
            $taosd -c ${dnode_dir}/config &> ${dnode_dir}/output.txt &
            echo $! > ${dnode_dir}/dnodePid
        fi
    else
        # Set up ip address
        if_name="lo:${idnode}"
        ip_addr="${ip_prefix}${idnode}"
        echo "Configuring ${if_name} : ${ip_addr} ..."
        sudo ifconfig ${if_name} down &> /dev/null || echo &> /dev/null
        sudo ifconfig ${if_name} ${ip_addr} up

        mkdir -p "${dnode_dir}/config"
        echo $ip_addr > ${dnode_dir}/dnodeIP
        echo $if_name > ${dnode_dir}/dnodeIF

        # set configure file
        f_config="${dnode_dir}/config/taos.cfg"
        echo "masterIp              ${ip_prefix}1"       > ${f_config}
        echo "publicIp              ${ip_addr}"         >> ${f_config}
        echo "privateIP             ${ip_addr}"         >> ${f_config}
        echo "internalIP            ${ip_addr}"         >> ${f_config}
        echo "dataDir               ${dnode_dir}/data"  >> ${f_config}
        echo "logDir                ${dnode_dir}/log"   >> ${f_config}
        echo "dDebugFlag            ${debug_flag}"      >> ${f_config}
        echo "mDebugFlag            ${debug_flag}"      >> ${f_config}
        echo "sdbDebugFlag          ${debug_flag}"      >> ${f_config}
        echo "rpcDebugFlag         ${debug_flag}"      >> ${f_config}
        echo "cDebugFlag            ${debug_flag}"      >> ${f_config}
        echo "gcDebugFlag           ${debug_flag}"      >> ${f_config}
        echo "adminDebugFlag        ${debug_flag}"      >> ${f_config}
        echo "httpDebugFlag         ${debug_flag}"      >> ${f_config}
        echo "restDebugFlag         ${debug_flag}"      >> ${f_config}
        echo "DebugFlag             ${debug_flag}"      >> ${f_config}

        # Add additional config
        if [ -e taos.cfg ]; then
            cat taos.cfg >> ${f_config}
        fi

        # Start the service
        if [ $2 = '1' ]; then
            $taosd -c ${dnode_dir}/config &> ${dnode_dir}/output.txt &
            echo $! > ${dnode_dir}/dnodePid
        fi
    fi

    sleep 0.5

    if [ $2 = '1' ]; then
        if is_dnode_running  $idnode; then
            return 0
        fi
    else
        return 0
    fi

    return 1
}


function drop_dnode() {
    idnode=$1
    stop_dnode $idnode 15 || return 1

    dnode_dir=${dnode_home}/dnode${idnode}
    rm -rf $dnode_dir
}

function dnode_status() {
    dnode_dir=$1
    pid_file=${dnode_dir}/dnodePid
    if [ -f $pid_file ]; then
        pid=$(cat ${pid_file})
        ip=$(cat ${dnode_dir}/dnodeIP)
        if grep -q taosd /proc/$pid/cmdline &> /dev/null; then
            printf "${BROWN_COLOR}%-10s${BLUE_COLOR}%-16s%-12s${GREEN_COLOR}%-3s\n${NO_COLOR}" "$(basename $dnode_dir)"\
                "${ip}"\
                "PID:${pid}"\
                "on"
        else
            printf "${BROWN_COLOR}%-10s${BLUE_COLOR}%-16s%-12s${RED_COLOR}%-3s\n${NO_COLOR}" "$(basename $dnode_dir)"\
                "${ip}"\
                "PID:${pid}"\
                "off"
        fi

        return 0
    fi
    echo -e "${BROWN_COLOR}$(basename $dnode_dir)${NO_COLOR}    ${RED_COLOR}off${NO_COLOR}"
}

# -------------------------------------------------------------------------------

if [ "$#" -lt 1 ]; then
    print_help
    exit 0
fi


case "$1" in
    setup) # dnode setup [3] [is_run] [131]
        cleanup all
        nDnodes=3
        if [ ! -z $2 ]; then
            nDnodes=$2
        fi

        is_run=1
        if [ ! -z $3 ]; then
            is_run=0
        fi

        debug_flag=135
        if [ ! -z $4 ];then
            debug_flag=$4
        fi

        for ((i=1;i<=$nDnodes;i++)); do
            if ! start_dnode $i $is_run $debug_flag; then
                echo "Failed to start dnode$i"
                exit 1
            fi
            if [ -z $3 ]; then
                dnode_dir=${dnode_home}/dnode${idnode}
                [ $i -gt 1 ] && $taos -s "create dnode ${ip_prefix}$i" -c ${dnode_home}/dnode1/config -h ${ip_prefix}1 &> /dev/null || echo &> /dev/null
                rm -f ${dnode_home}/dnode1/log/taoslog* || :
            fi
        done
        ;;
    start) # dnode start 1 [131]
        if [ -z $2 ]; then
            echo "Please assign dnode number to start"
            echo "Example: dnode start 1 [131]"
            exit 1
        fi

        debug_flag=135
        if [ ! -z $3 ];then
            debug_flag=$3
        fi

        if ! start_dnode $2 1 $debug_flag; then
            exit 1
        fi

        exit 0
        ;;
    stop) # dnode stop 1 [9]
        if [ -z $2 ];then
            echo "Please assign dnode number to stop"
            echo "Example: dnode stop 1 [9]"
            exit 1
        fi

        idnode=$2
        test ! -z $3 && signum=$3 || signum=15

        stop_dnode $idnode $signum && exit 0 || exit 1
        ;;
    status) # dnode status TODO: dnode status [1]
        for dnode_dir in $dnode_home/dnode*; do
            dnode_status $dnode_dir
        done
        ;;
    clean) # dnode clean
        if [ -z $2 ];then
            cleanup
        else
            cleanup $2
        fi
        ;;
    restart) # dnode restart 3
        if [ -z $2 ]; then
            echo "Please assign dnode number to restart"
            echo "Example: dnode restart 1"
            exit 1
        fi
        idnode=$2
        if ! stop_dnode $idnode 15; then
            echo "Failed to stop dnode$idnode"
            exit 1
        fi
        sleep 0.5
        if ! start_dnode $idnode 1; then
            echo "Failed to start dnode$idnode"
            exit 1
        fi
        ;;
    drop) # dnode drop 1
        if [ -z $2 ]; then
            echo "Please assign dnode number to drop"
            echo "Example: dnode drop 2"
            exit 1
        fi
        drop_dnode $2 || exit 1
        ;;
    connect) # dnode connect 1
        test -z $2 && idnode=1 || idnode=$2
        dnode_dir=${dnode_home}/dnode${idnode}
        dnodeIP=${dnode_dir}/dnodeIP
        [ -f ${dnodeIP} ] && ip_addr=$(cat ${dnode_dir}/dnodeIP) || (echo -e "${RED_COLOR}dnode$idnode is not running${NO_COLOR}"; exit 1)
        $taos -c ${dnode_home}/dnode${idnode}/config -h $ip_addr || (echo -e "${RED_COLOR}dnode$idnode is not running${NO_COLOR}"; exit 1)
        ;;
    clear)
        for dnode_dir in $dnode_home/dnode*; do
            if [ -d $dnode_dir ]; then
                idnode=$(echo $(basename $dnode_dir) | tr -dc '0-9')
                if is_dnode_running $idnode; then
                    echo -e "${RED_COLOR}DNODE $idnode is still running!${NO_COLOR}"
                    exit 1
                fi
                rm -rf ${dnode_home}/dnode${idnode}/log ${dnode_home}/dnode${idnode}/data
            fi
        done
        ;;
    reset)
        count=0
        for dnode_dir in $dnode_home/dnode*; do
            if [ -d $dnode_dir ]; then
                count=$((count+1))
                idnode=$(echo $(basename $dnode_dir) | tr -dc '0-9')
                if is_dnode_running $idnode; then
                    $0 stop $idnode
                fi
                rm -rf ${dnode_home}/dnode${idnode}/log ${dnode_home}/dnode${idnode}/data
            fi
        done
        for dnode_dir in $dnode_home/dnode*; do
            if [ -d $dnode_dir ]; then
                idnode=$(echo $(basename $dnode_dir) | tr -dc '0-9')
                $0 start $idnode
            fi
        done
        ;;
    *)
        print_help
        exit 0
esac

