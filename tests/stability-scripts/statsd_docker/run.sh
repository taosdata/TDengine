#!/bin/bash
docker run -itd --name statsd_agent1 -h statsd_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6044 taosadapter_statsd:v1 /bin/bash
