#!/bin/bash

HBASE_DATA_HOME="/home/taos/opentsdb"

sudo service opentsdb stop

sleep 3

stop-hbase.sh

sleep 3

rm -rf ${HBASE_DATA_HOME}/*

sleep 1

start-hbase.sh

sleep 5

env COMPRESSION=NONE HBASE_HOME=/home/taos/local/hbase-1.4.8/ /usr/share/opentsdb/tools/create_table.sh
# /usr/share/opentsdb/tools/create_table.sh

sleep 3

sudo service opentsdb start

sleep 1
