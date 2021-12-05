#!/bin/bash
docker run -itd --name icinga2_agent1 -h icinga2_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6048 -e Icinga2Interval=1s taosadapter_icinga2:v1 /bin/bash
