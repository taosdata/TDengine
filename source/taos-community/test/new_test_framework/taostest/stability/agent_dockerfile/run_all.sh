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
    echo "eg: ./run_all.sh 10 10 1 10 50 10000:10010"
    exit 0
;;
esac
collectd_count=$1
icinga2_count=$2
statsd_count=$3
tcollector_count=$4
telegraf_count=$5
node_exporter_count=$6
taosadapter1_ip=172.26.10.86
taosadapter2_ip=172.26.10.85
taosadapter3_ip=172.26.10.84
./collectd_docker/run_collectd.sh $1 taosadapter1_collectd_agent* $taosadapter1_ip 6047 1
./icinga2_docker/run_icinga2.sh $2 taosadapter1_icinga2_agent* $taosadapter1_ip 6048 1
./statsd_docker/run_statsd.sh $3 taosadapter1_statsd_agent $taosadapter1_ip 6044
./tcollector_docker/run_tcollector.sh $4 taosadapter1_tcollector_agent* $taosadapter1_ip 6049
./telegraf_docker/run_telegraf.sh $5 taosadapter1_telegraf_agent* $taosadapter1_ip 6041 10s taosadapter1_telegraf

./collectd_docker/run_collectd.sh $1 taosadapter2_collectd_agent* $taosadapter2_ip 6047 1
./icinga2_docker/run_icinga2.sh $2 taosadapter2_icinga2_agent* $taosadapter2_ip 6048 1
./statsd_docker/run_statsd.sh $3 taosadapter2_statsd_agent $taosadapter2_ip 6044
./tcollector_docker/run_tcollector.sh $4 taosadapter2_tcollector_agent* $taosadapter2_ip 6049
./telegraf_docker/run_telegraf.sh $5 taosadapter2_telegraf_agent* $taosadapter2_ip 6041 10s taosadapter2_telegraf

./collectd_docker/run_collectd.sh $1 taosadapter3_collectd_agent* $taosadapter3_ip 6047 1
./icinga2_docker/run_icinga2.sh $2 taosadapter3_icinga2_agent* $taosadapter3_ip 6048 1
./statsd_docker/run_statsd.sh $3 taosadapter3_statsd_agent $taosadapter3_ip 6044
./tcollector_docker/run_tcollector.sh $4 taosadapter3_tcollector_agent* $taosadapter3_ip 6049
./telegraf_docker/run_telegraf.sh $5 taosadapter3_telegraf_agent* $taosadapter3_ip 6041 10s taosadapter3_telegraf

./node_exporter_docker/run_node_exporter.sh $6 node_exporter_agent*
