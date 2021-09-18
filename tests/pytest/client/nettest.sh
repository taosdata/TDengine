#!/bin/bash


taos -n speed -P 6030 -N 1 -l 1024 -S udp
taos -n speed -P 6030 -N 1 -l 1024 -S tcp
taos -n fqdn
