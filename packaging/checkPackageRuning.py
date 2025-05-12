#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# install pip
# pip install src/connector/python/

# -*- coding: utf-8 -*-
import sys , os
import getopt
import subprocess
# from this import d
import time


if( len(sys.argv)>1 ):
    serverHost=sys.argv[1]
else:
    serverHost="localhost"


# install taospy

out = subprocess.getoutput("pip3 show taospy|grep Version| awk -F ':' '{print $2}' ")
print("taospy version %s "%out)
if (out == "" ):
    os.system("pip3 install git+https://github.com/taosdata/taos-connector-python.git")
    print("install taos python connector")
else:
    os.system("pip3 install --upgrade  taospy  ")



# start taosd prepare 
# os.system("rm -rf /var/lib/taos/*")
# os.system("systemctl restart taosd ")

# wait a moment ,at least 10 seconds
time.sleep(10)

# prepare data by taosBenchmark 

os.system("taosBenchmark -y -n 100 -t 100 -h %s "%serverHost )

import taos

conn = taos.connect(host="%s"%serverHost,
                                         user="root",
                                         password="taosdata",
                                         database="test",
                                         port=6030,
                                         config="/etc/taos",  # for windows the default value is C:\TDengine\cfg
                                         timezone="Asia/Shanghai")  # default your host's timezone

server_version = conn.server_info
print("server_version", server_version)
client_version = conn.client_info
print("client_version", client_version)  # 3.0.0.0

# Execute a sql and get its result set. It's useful for SELECT statement
result: taos.TaosResult = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosBenchmark work not as expected ")
    sys.exit(1)
else:
    print(" taosBenchmark work as expected ")

# test taosdump dump out data and dump in data 

# dump out datas
os.system("taosdump --version")
os.system("mkdir -p /tmp/dumpdata")
os.system("rm -rf /tmp/dumpdata/*")



# dump data out 
print("taosdump dump out data")

os.system("taosdump -o /tmp/dumpdata -D test -h %s  "%serverHost)

# drop database of test
print("drop database test")
os.system(" taos -s ' drop database test ;'  -h %s  "%serverHost)

# dump data in 
print("taosdump dump data in")
os.system("taosdump -i /tmp/dumpdata -h %s  "%serverHost)

result = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosdump work not as expected ")
    sys.exit(1)
else:
    print(" taosdump work as expected ")

conn.close()