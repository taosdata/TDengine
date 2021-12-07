#!/bin/bash
case "$1" in
    -h|--help)
    echo "Usage:"
    echo "1st arg: collectd_count"
    echo "2nd arg: icinga2_count"
    echo "3rd arg: statsd_count"
    echo "4th arg: tcollector_count"
    echo "5th arg: telegraf_count"
    echo "6th arg: node_exporter port range"
    echo "eg: ./run_all.sh 10 10 1 10 100 10000:10010"
    exit 0
;;
esac
collectd_count=$1
icinga2_count=$2
statsd_count=$3
tcollector_count=$4
telegraf_count=$5
node_exporter_count=$6
./collectd_docker/run_collectd.sh $1 collectd_agent* 172.26.10.86 6047 1
./icinga2_docker/run_icinga2.sh $2 icinga2_agent* 172.26.10.86 6048 1
./statsd_docker/run_statsd.sh $3 statsd_agent 172.26.10.86 6044
./tcollector_docker/run_tcollector.sh $4 tcollector_agent* 172.26.10.86 6049
./telegraf_docker/run_telegraf.sh $5 telegraf_agent* 172.26.10.86 6041 1s telegraf
./node_exporter_docker/run_node_exporter.sh $6 node_exporter_agent*
