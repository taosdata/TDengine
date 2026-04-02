#!/bin/bash
ps -ef|grep taos|grep -v grep|cut -c 9-16|xargs kill -9
ps -ef|grep python3|grep -v grep|cut -c 9-16|xargs kill -9
ps -ef|grep taosBenchmark|grep -v grep|cut -c 9-16|xargs kill -9
ps -ef|grep taosd|grep -v grep|cut -c 9-16|xargs kill -9

cd /root/stability_test/data3
rm -rf ta*/*
cd /root/stability_test/log
rm -rf *
cd /root/stability_test/log3
rm -rf ta*/*