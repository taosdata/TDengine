#!/bin/bash
docker run -itd --name node_exporter_agent1 -h node_exporter_agent1 -p 10000:9100 taosadapter_node_exporter:v1 /bin/bash
