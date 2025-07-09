#!/bin/bash
./collectd_docker/run_collectd.sh rm collectd_agent*
./icinga2_docker/run_icinga2.sh rm icinga2_agent*
./statsd_docker/run_statsd.sh rm statsd_agent*
./tcollector_docker/run_tcollector.sh rm tcollector_agent*
./telegraf_docker/run_telegraf.sh rm telegraf_agent*
./node_exporter_docker/run_node_exporter.sh rm node_exporter_agent*
