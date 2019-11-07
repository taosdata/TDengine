#!/bin/bash
# ====================================================
# Script to test the performance of TDengine.
# ====================================================

set -e
# set -x


# ============================ Configurable parameters. ============================
nservers=3                        # Number of server
replica=3                         # Number of database replica
nclients=1                        # Number of client
nrecords=10000000                  # Number of records to insert
nrecords_per_request=100          # Number of records per request (max number 126)
test_time=1                       # Test time
query_interval=1000000
query_size=1000000
# ==================================================================================

# Connection info
master_ip="192.168.0.1"
db_prefix="db"                                                  
table_prefix="t"

# Fixed parameters
RED_COLOR='\033[0;31m'
GREEN_COLOR='\033[0;32m'
NO_COLOR='\033[0m'
nCores=$(grep -c ^processor /proc/cpuinfo) # number of cores

is_accurate="true"
# if [ $nservers -gt $nCores ]; then
#     echo -e "${RED_COLOR}ERROR: Server number larger than CPU number.$NO_COLOR"
#     exit 1
# fi

if [ $nCores -le 1 ] || [ $(($nservers+$nclients)) -gt $nCores ]; then
    echo -e "${RED_COLOR}WARNING: CPU number is smaller than the total number of client and server. Test results may be inaccurate.$NO_COLOR"
    is_accurate="false"
fi


# Make the interface
script_home=$(readlink -f "$(dirname $(readlink -f $0))/..")
dnode="${script_home}/scripts/dnode.sh"

root_home=$(readlink -f ${script_home}/../..)
dnode_home=${root_home}/dnodes
testTBase="${root_home}/build/bin/testTBase"
output_file=${root_home}/test_result

configDir=${dnode_home}/dnode1/config

if [ ! -f $testTBase ];then
    make -C ${script_home}/code &> /dev/null
fi


# read -p "Please enter your password to run this script: " -s passWord
# echo "" 
echo -e "${GREEN_COLOR}Script is running, please wait...${NO_COLOR}"


# Function declarations -----------------------------------------
function assign_core() {
    if [ is_accurate = "true" ]; then
        sudo taskset -acp $2 $1 > /dev/null
    fi
}

# Do the test. --------------------------------------------------
## Check configuration

mkdir -p $(dirname $output_file)


echo "#########################################################" >> $output_file
echo "# Server number:              $nservers" >> $output_file
echo "# Client number:              $nclients" >> $output_file
echo "# Records/Request             $nrecords_per_request" >> $output_file
echo "# Database replica:           $replica" >> $output_file
echo "# Test time:                  $(date +'%Y-%m-%d %T')" >> $output_file
echo "#########################################################" >> $output_file
echo "" >> $output_file
echo 'TB ||  WTime(s) |  WRecords  | WSpeed(R/s)|   WLatency(ms)  ||  QTime(s)  |  QRecords  | QSpeed(R/s) |QLatency(ms)||     Status' >> $output_file
echo "===============================================================================================================================" >> $output_file

## Launch test
for ((i=1;i<=$test_time;i++)); do
    # Set up environments
    $dnode clean
    $dnode setup $nservers 135

    for ((j=1;j<=$nservers;j++)); do
        pid=$(cat ${dnode_home}/dnode${j}/dnodePid)
        assign_core $pid $(($nCores-$j))
    done

    db_name=$db_prefix

    # echo $db_name

    sleep 1

    $testTBase $master_ip $nclients $db_name $table_prefix $nrecords $nrecords_per_request $query_interval $query_size 0 $replica $configDir &>> $output_file &
    pid=$!
    assign_core $pid 0-$(($nCores-$nservers-1))
    wait $pid
    echo "-------------------------------------------------------------------------------------------------------------------------------" >> $output_file
done
echo "" >> $output_file
echo "" >> $output_file
echo "" >> $output_file

$dnode clean all
# sudo systemctl restart taosd
