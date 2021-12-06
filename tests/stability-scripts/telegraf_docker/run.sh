#!/bin/bash
docker run -itd --name telegraf_agent1 -h telegraf_agent1 -e TaosadapterIp=172.26.10.86 -e TaosadapterPort=6041 -e TelegrafInterval=1s -e Dbname=telegraf taosadapter_telegraf:v1 /bin/bash
