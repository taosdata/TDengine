#!/bin/bash

set -e

base_home=$(readlink -f "$(dirname $(readlink -f $0))/..")

dnode="$base_home/scripts/dnode.sh"

# Configure environment
$dnode clean
$dnode start 1


# Do operations here.

db_name="db1"
table_name="t1"
records_per_request=10
insert_time=300              # seconds
wquery_interval=0             # sedcond
rquery_interval=0             # sedcond
ip_addr=192.168.0.1


# Start to write data
${base_home}/bin/insertDataForPeriod  $db_name $table_name $records_per_request $insert_time $wquery_interval $ip_addr &

${base_home}/bin/queryDataConsecutively $db_name $table_name  $rquery_interval $ip_addr
