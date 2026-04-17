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
import sys, os
import getopt
import shutil
import tempfile
import re
subprocess = __import__('subprocess')
# from this import d
import time


if( len(sys.argv)>1 ):
    serverHost=sys.argv[1]
    if not re.match(r'^[a-zA-Z0-9._-]+$', serverHost):
        print("Error: Invalid server host: only alphanumeric characters, dots, hyphens and underscores are allowed")
        sys.exit(1)
else:
    serverHost="localhost"


# install taospy

pip3_show = subprocess.run(["pip3", "show", "taospy"], capture_output=True, text=True)
out = ""
for line in pip3_show.stdout.splitlines():
    if line.startswith("Version"):
        out = line.split(":", 1)[1].strip()
        break
print("taospy version %s " % out)
if (out == "" ):
    subprocess.run(["pip3", "install", "git+https://github.com/taosdata/taos-connector-python.git"], check=False)
    print("install taos python connector")
else:
    subprocess.run(["pip3", "install", "--upgrade", "taospy"], check=False)



# start taosd prepare 
# os.system("rm -rf /var/lib/taos/*")
# os.system("systemctl restart taosd ")

# wait a moment ,at least 10 seconds
time.sleep(10)

# prepare data by taosBenchmark 

subprocess.run(["taosBenchmark", "-y", "-n", "100", "-t", "100", "-h", serverHost], check=False)

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
subprocess.run(["taosdump", "--version"], check=False)
dump_dir = tempfile.mkdtemp(prefix="tdengine_dump_")



# dump data out 
print("taosdump dump out data")

subprocess.run(["taosdump", "-o", dump_dir, "-D", "test", "-h", serverHost], check=False)

# drop database of test
print("drop database test")
subprocess.run(["taos", "-s", "drop database test ;", "-h", serverHost], check=False)

# dump data in 
print("taosdump dump data in")
subprocess.run(["taosdump", "-i", dump_dir, "-h", serverHost], check=False)

result = conn.query("SELECT count(*) from test.meters")

data = result.fetch_all()

if data[0][0] !=10000:
    print(" taosdump work not as expected ")
    sys.exit(1)
else:
    print(" taosdump work as expected ")

conn.close()

# clean up temporary dump directory
shutil.rmtree(dump_dir, ignore_errors=True)