#!/bin/bash
docker run -itd --name collectd_agent1 -h collectd_agent1 -e CollectdHostname=collectd_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6047 -e CollectdInterval=1 taosadapter_collectd:v1 /bin/bash
