#!/bin/bash
docker run -itd --name tcollector_agent1 -h tcollector_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6049  taosadapter_tcollector:v1 /bin/bash
