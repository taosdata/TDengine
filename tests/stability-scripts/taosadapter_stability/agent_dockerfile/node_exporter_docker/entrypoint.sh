#!/bin/bash
nohup /usr/bin/node_exporter &
tail -f /dev/null
